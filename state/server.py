from random import uniform
from select import select
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR
from time import time
from typing import Optional

from pydantic import ValidationError, conint

from conf import ServerConfig, app_cfg
from db import Entry
from log import logger
from role import Role, Candidate, Follower, Leader
from role.rpc import VoteReq, VoteResp, AppendEntryReq, AppendEntryResp, PutDataReq, PutDataResp
from util import Address, RPC, RPC_Direction, RPC_Type, FrozenModel


class _CaptureTerm(FrozenModel):
    # 可选字段 防止反序列化时失败报错
    term: Optional[int] = None


class Server:
    """
    节点作为Leader向集群其他节点发送的复制日志数量
    key-the Follower
    val-count for Append Entries which have been sent to Follower
    """
    _peers_sent: dict[Address, int]
    sock: socket | None = None
    # 超时到期时间
    timeout: float = time() + uniform(app_cfg.timeout_second_lo, app_cfg.timeout_second_hi)
    # 当前节点角色类型
    _role: Role = Follower(commit_idx=0, last_idx=0)
    # 得票箱 当前节点竞选Leader收到有投票
    _votes: set[Address] = set()

    def __init__(self, my_id: int, peers: list[ServerConfig]):
        """
        构造方法
        """
        # 当前节点id
        self.my_id = my_id
        # 集群中各个节点的配置
        self.peers = peers
        # 当前节点的配置
        self.cfg = next((peer for peer in peers if peer.id == my_id), None)

    def __del__(self):
        pass

    def __enter__(self):
        self.open_sock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_sock()

    def open_sock(self) -> None:
        """
        实例化socket
        """
        try:
            # udp
            self.sock = socket(AF_INET, SOCK_DGRAM)
            self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            self.sock.bind((self.cfg.ip, self.cfg.port))
        except Exception as e:
            logger.error(f"实例化socket失败{e}")
            exit(1)

    def close_sock(self) -> None:
        logger.info("开始关闭socket")
        if self.sock:
            self.sock.close()

    def is_time_out(self) -> bool:
        """超时到期"""
        return time() > self.timeout

    def is_leader(self) -> bool:
        """角色类型"""
        return isinstance(self._role, Leader)

    def start_heartbeat(self) -> None:
        """向follower发送心跳 维持自己leader地位"""
        # 角色类型检查
        if not isinstance(self._role, Leader):
            return
        match_indices = self._role.match_idx.values()
        n: int = min(match_indices)
        while (
                len(self._role.logs) > n > self._role.commit_idx
                and sum(idx >= n for idx in match_indices) * 2 > len(self.peers)
                and self._role.logs[n].term == self._role.cur_term
        ):
            self._role.commit_idx = n
            n += 1
        self._rpc_send_append_entries()
        self._timeout_reset(leader=True)

    def start_election(self) -> None:
        """Follower超时到了 说明Leader挂了 自己开始竞选Leader"""
        logger.info(f'节点{self.my_id}开始拉票')
        if self.sock is None:
            return
        # 切换到candidate
        self._role.update_cur_term(self._role.cur_term + 1)
        self._role_promote_to_candidate()
        self_sock_addr: Address = self._id()
        self._role.update_voted_for(self_sock_addr)
        # 得票箱 每一轮选举都重置一下
        self._votes = {self_sock_addr}
        self._timeout_reset()
        # 要socket发送的信息
        rpc: RPC = RPC(
            direction=RPC_Direction.REQ,
            type=RPC_Type.REQUEST_VOTE,
            content=VoteReq(
                term=self._role.cur_term,
                candidate_identity=self_sock_addr,
                last_log_idx=self._role.logs[-1].index,
                last_log_term=self._role.logs[-1].term,
            ).json()
        )
        # 自己给自己的投票已经在内存中处理过了 不需要走网路
        [self._rpc_send(rpc, Address(host=peer.ip, port=peer.port)) for peer in self.peers if peer.id != self.my_id]

    def _rpc_send(self, rpc: RPC, recv: Address) -> None:
        """
        socket发送消息
        Args:
            rpc 发送什么消息
            recv 发送给谁
        """
        if self.sock is None:
            logger.error("socket没有初始化 没法发送消息")
        else:
            try:
                logger.info(f'向{recv}发送消息{rpc}')
                self.sock.sendto(f"{rpc.json()}\n".encode(), (recv.host, recv.port))
            except Exception as e:
                logger.error(f"socket发送失败{e}")

    def rpc_handle(self, rpc: RPC, sender: Address) -> None:
        """
        收到了来自别人的请求
        Args:
            rpc 收到的数据
            sender 谁发来的
        """
        logger.info(f'主机{self.my_id}收到{sender.host}:{sender.port}的数据是{rpc} 开始处理')
        try:
            self._role_demote_if_necessary(_CaptureTerm.parse_raw(rpc.content))
            if rpc.direction == RPC_Direction.REQ:
                res: RPC
                match rpc.type:
                    case RPC_Type.REQUEST_VOTE:
                        # 处理别人拉票
                        res = self._rpc_handle_vote_req(VoteReq.parse_raw(rpc.content))
                    case RPC_Type.APPEND_ENTRIES:
                        # Candidate或Follower收到了Leader的Append Entries
                        res = self._rpc_handle_append_entries_req(AppendEntryReq.parse_raw(rpc.content))
                    case RPC_Type.CLIENT_QUERY:
                        pass
                    case RPC_Type.CLIENT_PUT:
                        res = self._rpc_handle_client_put_req(PutDataReq.parse_raw(rpc.content));
                    case RPC_Type.CLIENT_PUT_FORWARD:
                        res = self._rpc_handle_client_put_forward_req(PutDataReq.parse_raw(rpc.content));
                    case _:
                        raise NotImplementedError(f"rpc type={rpc.type}尚不支持")
                if isinstance(res, RPC):
                    self._rpc_send(res, sender)
            elif rpc.direction == RPC_Direction.RESP:
                match rpc.type:
                    case RPC_Type.REQUEST_VOTE:
                        self._rpc_handle_vote_response(VoteResp.parse_raw(rpc.content), sender)
                    case RPC_Type.APPEND_ENTRIES:
                        self._rpc_handle_append_entries_response(AppendEntryResp.parse_raw(rpc.content), sender)
                    case RPC_Type.CLIENT_QUERY:
                        pass
                    case RPC_Type.CLIENT_PUT | RPC_Type.CLIENT_PUT_FORWARD:
                        pass
                    case _:
                        raise NotImplementedError(f"rpc type={rpc.type}尚不支持")
        except ValidationError as e:
            logger.error(f"收到的请求不合法{e}")
        except NotImplementedError as e:
            logger.error(f"异常{e}")
        except:
            logger.error("未知异常")

    def _rpc_handle_append_entries_req(self, req: AppendEntryReq) -> RPC:
        """
        对于Leader而言核心就是Append Entries RPC的调用
        这个调用承载着3个作用
        1 心跳heartbeat---如果没有新的entry 也会定期发送空的AppendEntries以维持领导地位
        2 复制日志entry---把客户端请求封装的entry发送出去 等待follower追加
        3 提交日志commit---告诉follower哪些entry已被集群大多数节点确认并可以提交到状态机

        follower收到这些entry后先追加日志
        然后follower执行完这个方法会尝试扫描一下commit_idx看看是不是有新的待提交的entry

        Args:
            req(AppendEntryReq): leader发送给follower的append entry日志
                                 可能是简单的心跳作用
                                 可能是复制日志作用
                                 可能是提交日志作用
                                 具体作用就看请求中的几个字段标识语义就行
        Returns:
            None
        """
        res: AppendEntryResp
        prev_entry: Entry | None = None
        if 0 <= req.prev_log_idx < len(self._role.logs):
            prev_entry = self._role.logs[req.prev_log_idx]
        # 作为Follower收到Leader的Append Entries就说明Leader还存活着 他还有统治地位 自己就不要想着竞争选举了 重置计时器
        self._timeout_reset()
        if req.term < self._role.cur_term:
            # 过期消息
            res = AppendEntryResp(term=self._role.cur_term, succ=False)
        elif prev_entry is None or req.prev_log_term != prev_entry.term:
            res = AppendEntryResp(term=self._role.cur_term, succ=False)
        else:
            if not isinstance(self._role, Follower):
                # 集群刚选主成功 集群里面只有一个leader跟一群candidate 借着candidate收到Leader的Append Entry请求时机切换角色成Follower
                self._role_demote_to_follower()
            assert all(x.index + 1 == y.index for x, y in zip(req.entries, req.entries[1:]))
            # Follower把Leader同步过来的Append Entry保存等待Commit
            [self._role.update_log(entry) for entry in req.entries]
            # Leader告诉Follower哪些entry已经被集群大多数节点确认 Follower可以提交到状态机了
            if req.leader_commit_idx > self._role.commit_idx:
                logger.info(f'Leader告诉我的最新可以提交的index={req.leader_commit_idx}')
                # 有新的追加日志可以提交了 更新commit_idx 等待一会的扫描动作尝试提交
                self._role.commit_idx = min(req.leader_commit_idx, len(self._role.logs) - 1)
            res = AppendEntryResp(term=self._role.cur_term, succ=True)
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.APPEND_ENTRIES, content=res.json())

    def _rpc_handle_append_entries_response(self, res: AppendEntryResp, sender: Address) -> None:
        if not isinstance(self._role, Leader) or res.term != self._role.cur_term:
            return
        if res.succ:
            prev_log_idx: int = self._role.next_idx[sender] - 1
            num_entries: int = self._peers_sent[sender]
            if self._role.match_idx[sender] > prev_log_idx + num_entries:
                logger.warn(f'任期不对')
            else:
                self._role.next_idx[sender] = self._role.match_idx[sender] + 1
        elif self._role.next_idx[sender] > 1:
            self._role.next_idx[sender] -= 1

    def _rpc_handle_vote_req(self, req: VoteReq) -> RPC:
        """
        收到别人的拉票请求
        自己是leader就果断投反对票 一个集群不允许脑裂
        自己不是leader就看看对面有没有资格当leader决定自己是投反对票还是赞成票
        """
        logger.info(f'收到的拉票请求是{req}')
        last_entry: Entry | None = self._role.logs[-1]
        assert last_entry is not None
        # 反对票
        res: VoteResp = VoteResp(term=self._role.cur_term, vote_granted=False)
        if isinstance(self._role, Leader):
            logger.info(f'当前{self.my_id}是leader 集群中已经有leader 不同意拉票选举')
            # 投反对票
            return RPC(direction=RPC_Direction.RESP, type=RPC_Type.REQUEST_VOTE, content=res.json())
        self._timeout_reset()
        # 比较term和log看看对方有没有当leader资格
        at_least_as_up_to_date: bool = req.last_log_term > last_entry.term or (req.last_log_term == last_entry.term and req.last_log_idx >= last_entry.index)
        logger.info(f'拉票的人term是{req.term} 自己的term是{self._role.cur_term}')
        if req.term < self._role.cur_term:
            logger.info('拉票的term比自己低 投反对票')
            res = VoteResp(term=self._role.cur_term, vote_granted=False)
        elif at_least_as_up_to_date and (self._role.voted_for is None or self._role.voted_for == req.candidate_identity):
            self._role.update_voted_for(req.candidate_identity)
            # 投赞同票
            logger.info('拉票的term比自己高 投赞同票')
            res = VoteResp(term=self._role.cur_term, vote_granted=True)
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.REQUEST_VOTE, content=res.json())

    def _rpc_handle_vote_response(self, res: VoteResp, sender: Address) -> None:
        """
        收到了sender的投票结果res
        可能是反对票
        可能是赞同票 进行得票统计 看看自己有没有资格当leader
        """
        logger.info(f'当前{self._role}收到{sender.host}:{sender.port}的投票结果是{res}')
        if not isinstance(self._role, Candidate) or not res.vote_granted:
            logger.info(f'自己现在是{self._role} 别人投票结果是{res.vote_granted} 不统计得票了')
            return
        logger.info('拉到了一个赞同票')
        # 放到得票箱准备统计得票情况
        self._votes.add(sender)
        logger.info(f'现在总共{len(self._votes)}个赞同票 集群共{len(self.peers)}个节点')
        # 得票过半就晋升leader
        if len(self._votes) * 2 > len(self.peers):
            logger.info(f'晋升为Leader 同步日志到集群')
            self._role_promote_to_leader()
            # Leader发送Append Entries
            self._rpc_send_append_entries()
            self._timeout_reset(leader=True)

    def _rpc_send_append_entries(self) -> None:
        """
        Leader的核心功能 对于Leader而言核心就是Append Entries RPC的调用
        这个调用承载着3个作用
        1 心跳heartbeat---如果没有新的entry 也会定期发送空的AppendEntries以维持领导地位
        2 复制日志entry---把客户端请求封装的entry发送出去 等待follower追加
        3 提交日志commit---告诉follower哪些entry已被集群大多数节点确认并可以提交到状态机
        """
        if not isinstance(self._role, Leader):
            # 只有Leader才有资格发送Append Entries double check
            return
        for peer in self.peers:
            if self.my_id == peer.id: continue
            # Append Entries req, to send whom
            addr: Address = Address(host=peer.ip, port=peer.port)
            #
            prev_entry: Entry = self._role.logs[self._role.next_idx[addr] - 1]
            # all entries, leader has sync to follower
            entries: list[Entry] = self._role.logs[self._role.next_idx[addr]:]
            self._peers_sent[addr] = len(entries)
            # leader发送append entries
            self._rpc_send(
                rpc=RPC(
                    direction=RPC_Direction.REQ,
                    type=RPC_Type.APPEND_ENTRIES,
                    content=AppendEntryReq(
                        term=self._role.cur_term,
                        leader_identity=self._id(),
                        prev_log_idx=self._role.next_idx[addr] - 1,
                        prev_log_term=prev_entry.term,
                        entries=entries,
                        leader_commit_idx=self._role.commit_idx,
                    ).json()
                ),
                recv=addr
            )

    def commit(self) -> None:
        logger.info(f'当前节点角色是{self._role}开始执行提交')
        self._role.commit()

    def _timeout_reset(self, leader: bool = False) -> None:
        """重置当前节点的超时"""
        logger.info("开始重置超时")
        duration: float = uniform(app_cfg.timeout_second_lo, app_cfg.timeout_second_hi)
        if leader:
            duration /= 3
        self.timeout = time() + duration

    def _role_promote_to_candidate(self) -> None:
        """follower->candidate"""
        logger.info(f'切换到candidate准备竞选leader 当前角色{self._role}->Candidate')
        self._role = Candidate(**self._role.dict())

    def _role_promote_to_leader(self) -> None:
        """candidate->leader"""
        logger.info(f'当前角色{self._role}晋升为leader term is{self._role.cur_term}')
        self._role = Leader(**self._role.dict(),
                            next_idx={Address(host=peer.ip, port=peer.port): len(self._role.logs) for peer in self.peers},
                            match_idx={Address(host=peer.ip, port=peer.port): 0 for peer in self.peers}
                            )
        # 刚晋升Leader 作为Leader这个角色自然还没有向集群同步过日志 初始化计数都是0
        self._peers_sent = {Address(host=peer.ip, port=peer.port): 0 for peer in self.peers}

    def _id(self) -> Address:
        """当前节点的ip和port"""
        if self.sock is None:
            raise RuntimeError("当前节点的socket异常")
        host, port = self.sock.getsockname()
        return Address(host=host, port=port)

    def start(self) -> None:
        """
        核心逻辑
        """
        logger.info(f"当前节点{self.my_id}启动")
        try:
            while True:
                logger.info(f"还有{self.timeout - time():.2f}s到期 开始阻塞调用复用器")
                # io多路复用
                readable: list[socket]
                exceptional: list[socket]
                # 收到的网络请求 转发过来的客户端存\取 别人的拉票
                readable, _, exceptional = select([self.sock], [], [], max(0, self.timeout - time()))
                logger.info(f"复用器拿到的就绪事件是{len(readable)}个 异常事件{len(exceptional)}个")
                if self.is_time_out():
                    logger.info(f'已经到期')
                    self.start_heartbeat() if self.is_leader() else self.start_election()
                # 处理收到的请求
                [self._recv_sock(s) for s in readable]
                # re-bind
                [self.open_sock() for s in exceptional if s is self.sock]
                self.commit()
        except KeyboardInterrupt:
            print("服务正常退出")
        except Exception as e:
            print(f"服务执行异常{e}")
            raise e

    def _recv_sock(self, sock: socket) -> None:
        """
        当前节点raft端口收到的请求
        1 可能来自集群其他节点
        2 可能来自自身数据端口转发过来的客户端请求

        请求类型
        1 客户端的数据存\取
        2 拉票
        3 Append Entries
        """
        data: bytes
        addr: tuple[str, int]
        data, addr = sock.recvfrom(1 << 10)
        [self.rpc_handle(rpc=RPC.parse_raw(payload), sender=Address(host=addr[0], port=addr[1])) for payload in data.decode().splitlines(keepends=True)]

    def _role_demote_if_necessary(self, capture: _CaptureTerm) -> None:
        if capture.term is None or capture.term <= self._role.cur_term:
            return
        self._role.update_cur_term(capture.term)
        self._role.update_voted_for(None)
        if not isinstance(self._role, Follower):
            self._role_demote_to_follower()

    def _role_demote_to_follower(self) -> None:
        logger.info(f'当前角色{self._role}->Follower')
        self._role = Follower(**self._role.dict())

    def _rpc_handle_client_put_req(self, req: PutDataReq) -> RPC:
        """收到来自客户端的存数据请求"""
        logger.info("收到来自客户端存数据的请求")
        # Leader直接存到logs里面 等待下一轮的Append Entries同步给Leader
        if isinstance(self._role, Leader):
            self._role.update_log(Entry(index=len(self._role.logs), term=self._role.cur_term, key=req.key, value=req.val))
            return RPC(direction=RPC_Direction.RESP, type=RPC_Type.CLIENT_PUT)
        # 发布到集群 让Leader节点处理 刨除自己不然区分不出来是client转发过来还是Follower发布过来的
        rpc: RPC = RPC(
            direction=RPC_Direction.REQ,
            type=RPC_Type.CLIENT_PUT_FORWARD,
            content=PutDataReq(key=req.key, val=req.val).json()
        )
        [self.sock.sendto(f"{rpc.json()}\n".encode(), (peer.ip, peer.port)) for peer in self.peers if peer.id!=self.my_id]
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.CLIENT_PUT, content=None)

    def _rpc_handle_client_put_forward_req(self, req: PutDataReq) -> RPC:
        """集群节点转发过来的客户端的存数据请求"""
        logger.info("收到来自转发的存数据的请求")
        # Leader直接存到logs里面 等待下一轮的Append Entries同步给Leader
        if isinstance(self._role, Leader):
            self._role.update_log(Entry(index=len(self._role.logs), term=self._role.cur_term, key=req.key, value=req.val))
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.CLIENT_PUT_FORWARD, content=None)
