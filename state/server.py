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
    每一轮Leader向Follower同步日志时可能是单条 可能是批量 每一轮同步的时候记录向Follower同步了多少条
    """
    _peers_sent: dict[Address, int]
    sock: socket | None = None
    # 超时到期时间
    timeout: float = time() + uniform(app_cfg.timeout_second_lo, app_cfg.timeout_second_hi)
    # 启动的时候默认节点角色是Follower 一个超时周期后收不到Leader的RPC就会触发竞争选主
    _role: Role = Follower()
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
        """
        向Follower发送Append Entries 维持自己leader地位
        语义作用
        1 发送Leader的心跳
        2 Leader向Follower的Append Entries
        """
        # 角色类型检查
        if not isinstance(self._role, Leader):
            return
        # 集群里面已经收到自己要同步的最小log index 比这个日志大的可能丢了或者Follower异常没回复 初始化时全是-1
        cluster_confirm_sync_idxes = self._role.confirm_sync_idx.values()
        logger.info(f'集群节点确认同步的情况{self._role.confirm_sync_idx}')
        # [0...idx]整个集群都同步过
        # (idx...]可能有的节点同步过有的节点没同步过
        # 从已经同步过的最小的日志开始考察 看看哪些还没提交 推进Leader的commit
        # 后面Leader向Follower发送Append Entries RPC的时候带上这个信息让Follower开始以此为基准进行提交带着着集群推进commit
        # [ready_commit_idx....)都是可以提交的
        idx: int = min(cluster_confirm_sync_idxes)
        logger.info(f'整个集群确认通过的最小日志是{idx}')
        # 初始化时候是-1 需要特殊处理
        idx = max(idx, 0)
        logger.info(f'整个集群确认通过的最小日志修正后是{idx}')
        while (len(self._role.logs) > idx > self._role.pending_commit_idx_threshold  # 还没提交
               and sum(idx >= idx for idx in cluster_confirm_sync_idxes) * 2 > len(self.peers)  # 过半节点已经同步过idx
               and self._role.logs[idx].term == self._role.cur_term # 是自己任期同步的日志
        ):
            self._role.pending_commit_idx_threshold = idx
            logger.info(f'调整待提交日志点位为{self._role.pending_commit_idx_threshold}')
            idx+=1
        self._rpc_send_append_entries()
        self._timeout_reset(leader=True)

    def start_election(self) -> None:
        """Follower超时到了 说明Leader挂了 自己开始竞选Leader"""
        logger.info(f'节点{self.my_id}开始拉票 自己的term={self._role.cur_term}')
        if self.sock is None:
            return
        # 切换到Candidate
        # 初始化时保证了0-based 放心operate
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
                last_log_idx=self._role.logs[-1].index if self._role.logs else -1,
                last_log_term=self._role.logs[-1].term if self._role.logs else -1,
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
                logger.info(f'向{recv.host}:{recv.port}发送消息{rpc}')
                self.sock.sendto(f"{rpc.json()}\n".encode(), (recv.host, recv.port))
            except Exception as e:
                logger.error(f"socket发送失败{e}")

    def rpc_handle(self, rpc: RPC, sender: Address) -> None:
        """
        收到了来自别人的请求
        Leader 收到了来自Follower的Append Entries回复
               收到了来自Candidate的拉票
               收到了来自客户端的put
               收到了来自Follower转发的put
        Follower 收到了来自Leader的Append Entries请求
                 收到了来自Follower转发的put
        Args:
            rpc 收到的数据
            sender 谁发来的
        """
        logger.info(f'主机{self.my_id}收到{sender.host}:{sender.port}的数据是{rpc} 开始处理')
        try:
            self._role_demote_if_necessary(_CaptureTerm.parse_raw(rpc.content))
            if rpc.direction == RPC_Direction.REQ:
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
                        self._rpc_handle_vote_resp(VoteResp.parse_raw(rpc.content), sender)
                    case RPC_Type.APPEND_ENTRIES:
                        self._rpc_handle_append_entries_resp(AppendEntryResp.parse_raw(rpc.content), sender)
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

        Follower收到这些entry后先追加日志
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
        logger.info(f'处理Leader发来的Append Entries')
        res: AppendEntryResp
        # 收到了Leader的同步 看看自己上一条收到的同步日志
        prev_entry: Entry | None = None
        if self._role.logs and 0 <= req.prev_log_idx < len(self._role.logs):
            prev_entry = self._role.logs[req.prev_log_idx]
        # 作为Follower收到Leader的Append Entries就说明Leader还存活着 他还有统治地位 自己就不要想着竞争选举了 重置计时器
        self._timeout_reset()
        if req.term < self._role.cur_term:
            # 过期消息
            res = AppendEntryResp(term=self._role.cur_term, succ=False)
        elif prev_entry is not None and req.prev_log_term != prev_entry.term:
            res = AppendEntryResp(term=self._role.cur_term, succ=False)
        else:
            if not isinstance(self._role, Follower):
                # 集群刚选主成功 集群里面只有一个leader跟一群candidate 借着candidate收到Leader的Append Entry请求时机切换角色成Follower
                self._role_demote_to_follower()
            # 确保Leader同步过来的日志是严格递增的
            assert all(x.index + 1 == y.index for x, y in zip(req.entries, req.entries[1:]))
            # Follower把Leader同步过来的Append Entry保存等待Commit
            [self._role.update_log(entry) for entry in req.entries]
            # Leader告诉Follower哪些entry已经被集群大多数节点确认 Follower可以提交到状态机了
            if req.leader_commit_idx > self._role.pending_commit_idx_threshold:
                logger.info(f'Leader告诉我的最新可以提交的index={req.leader_commit_idx} 我自己最新的可提交index={self._role.pending_commit_idx_threshold}')
                # 有新的追加日志可以提交了 更新commit_idx 等待一会的扫描动作尝试提交
                self._role.pending_commit_idx_threshold = min(req.leader_commit_idx, len(self._role.logs) - 1)
            res = AppendEntryResp(term=self._role.cur_term, succ=True)
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.APPEND_ENTRIES, content=res.json())

    def _rpc_handle_append_entries_resp(self, res: AppendEntryResp, sender: Address) -> None:
        logger.info(f'收到了{sender.host}:{sender.port}的Append Entries的回复 结果是{res.succ}')
        if not isinstance(self._role, Leader) or res.term != self._role.cur_term:
            logger.error(f'当前节点不是Leader 任期对不上 放弃处理这条回复')
            return
        if res.succ:
            # Leader给Follower同步过的上一个日志
            prev_log_idx: int = self._role.next_sync_idx[sender] - 1
            # Leader给Follower同步过多少个日志
            sync_cnt: int = self._peers_sent[sender]
            logger.info(f'Append Entries回复成功 即Follower同步成功 给{sender.host}:{sender.port}同步的上一个日志是{prev_log_idx} 已经总共给它同步过{sync_cnt}个日志')
            # 更新同步确认的点位
            self._role.confirm_sync_idx[sender] = max(self._role.confirm_sync_idx[sender], prev_log_idx+sync_cnt)
            self._role.next_sync_idx[sender] = self._role.confirm_sync_idx[sender] + 1
        elif self._role.next_sync_idx[sender] > 0:
            logger.info(f'Append Entries回复失败 即Follower同步失败 ')
            self._role.next_sync_idx[sender] -= 1

    def _rpc_handle_vote_req(self, req: VoteReq) -> RPC:
        """
        收到别人的拉票请求
        自己是leader就果断投反对票 一个集群不允许脑裂
        自己不是leader就看看对面有没有资格当leader决定自己是投反对票还是赞成票
        """
        logger.info(f'收到来自{req.candidate_identity.host}:{req.candidate_identity.port}的拉票请求')
        # 刚启动初始化时候logs是空的
        last_entry: Entry | None = self._role.logs[-1] if self._role.logs else None
        if isinstance(self._role, Leader):
            logger.info(f'当前{self.my_id}是leader 集群中已经有leader 不同意拉票选举')
            # 投反对票
            return RPC(direction=RPC_Direction.RESP,
                       type=RPC_Type.REQUEST_VOTE,
                       content=VoteResp(term=self._role.cur_term, vote_granted=False).json()
                       )
        self._timeout_reset()
        # 自己是Candidate 比较term和log看看对方有没有资格当Leader
        qualified: bool = ((last_entry is None)
                                        or (req.last_log_term > last_entry.term)
                                        or (req.last_log_term == last_entry.term and req.last_log_idx >= last_entry.index)
                                        )
        logger.info(f'拉票的人term是{req.term} 自己的term是{self._role.cur_term}')
        res: VoteResp = VoteResp(term=self._role.cur_term, vote_granted=False)
        if req.term < self._role.cur_term:
            logger.info('拉票的term比自己低 投反对票')
        elif qualified and (self._role.voted_for is None or self._role.voted_for == req.candidate_identity):
            self._role.update_voted_for(req.candidate_identity)
            # 投赞同票
            logger.info('拉票的term比自己高 投赞同票')
            res.vote_granted=True
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.REQUEST_VOTE, content=res.json())

    def _rpc_handle_vote_resp(self, res: VoteResp, sender: Address) -> None:
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
            # 向集群里面Follower节点同步
            if self.my_id == peer.id: continue
            # Append Entries req, to send whom
            addr: Address = Address(host=peer.ip, port=peer.port)
            prev_log_term: int = -1
            prev_log_idx: int = -1
            entries: list[Entry] = []
            # 没有要同步的日志 Append Entries就退化成单纯的心跳 用来维持自己的Leader地位
            if self._role.logs:
                logger.info(f'当前角色{self._role} next_sync_idx={self._role.next_sync_idx} logs={self._role.logs}')
                # Leader向Follower同步过的最新的日志是哪个
                prev_log_idx = self._role.next_sync_idx[addr] - 1
                prev_entry: Entry = self._role.logs[prev_log_idx]
                prev_log_term = prev_entry.term
                # 这一轮准备向Follower同步的所有日志
                entries = self._role.logs[prev_log_idx+1:]
                # 缓存这一轮向Follower同步了多少条日志
                self._peers_sent[addr] = len(entries)
            # leader发送append entries
            self._rpc_send(
                rpc=RPC(
                    direction=RPC_Direction.REQ,
                    type=RPC_Type.APPEND_ENTRIES,
                    content=AppendEntryReq(
                        term=self._role.cur_term,
                        leader_identity=self._id(),
                        prev_log_idx=prev_log_idx,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit_idx=self._role.pending_commit_idx_threshold,
                    ).json()
                ),
                recv=addr
            )

    def commit(self) -> None:
        logger.info(f'当前节点角色是{self._role}开始尝试提交日志')
        self._role.commit()
        logger.info(f'当前节点角色是{self._role}结束提交日志')

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
        """
        Candidate->Leader
        Candidate刚晋升Leader 初始化log指针
        1 待同步日志
        2 已同步日志
        """
        logger.info(f'当前角色{self._role}晋升为leader term is {self._role.cur_term}')
        self._role = Leader(**self._role.dict(),
                            next_sync_idx={Address(host=peer.ip, port=peer.port): len(self._role.logs) for peer in self.peers},
                            confirm_sync_idx={Address(host=peer.ip, port=peer.port): -1 for peer in self.peers}
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
                logger.info(f"开始时间循环 有{self.timeout - time():.2f}s到期 开始阻塞调用复用器")
                # io多路复用
                readable: list[socket]
                exceptional: list[socket]
                # 收到的网络请求 转发过来的客户端存\取 别人的拉票 Follower的Append Entries回复 Follower的commit回复
                readable, _, exceptional = select([self.sock], [], [], max(0, self.timeout - time()))
                logger.info(f"复用器拿到的就绪事件是{len(readable)}个 异常事件{len(exceptional)}个")
                if self.is_time_out():
                    logger.info(f'已经到期')
                    self.start_heartbeat() if self.is_leader() else self.start_election()
                # 处理收到的请求
                [self._recv_sock(s) for s in readable]
                # re-bind
                [self.open_sock() for s in exceptional if s is self.sock]
                # 尝试执行日志提交
                self.commit()
                logger.info(f'结束事件循环')
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
        """
        从PRC中反序列化出来了term
        客户端put请求没有term值
        初始化时term=-1
        """
        logger.info(f'RPC的任期{capture} 自己的term={self._role.cur_term}')
        if capture.term is None or capture.term <= self._role.cur_term:
            return
        self._role.update_cur_term(capture.term)
        self._role.update_voted_for(None)
        if not isinstance(self._role, Follower):
            # Leader收到的RPC出现了比自己大和term Leader降级为Follower
            self._role_demote_to_follower()

    def _role_demote_to_follower(self) -> None:
        logger.info(f'当前角色{self._role}->Follower')
        self._role = Follower(**self._role.dict())

    def _rpc_handle_client_put_req(self, req: PutDataReq) -> RPC:
        """收到来自客户端的存数据请求"""
        logger.info("收到来自客户端存数据的请求")
        # Leader直接存到logs里面 等待下一轮的Append Entries同步给Leader
        if isinstance(self._role, Leader):
            self._role.update_log(Entry(index=len(self._role.logs) if self._role.logs else 0, term=self._role.cur_term, key=req.key, value=req.val))
            # Leader自己节点的同步和同步确认点位更新
            cur: Address = self._id()
            # 给Leader自己添加了日志量
            sync_cnt: int = 1
            self._peers_sent[cur] = sync_cnt
            prev_log_idx: int = self._role.next_sync_idx[cur]-1
            # 更新同步确认的点位
            self._role.confirm_sync_idx[cur] = max(self._role.confirm_sync_idx[cur], prev_log_idx+sync_cnt)
            self._role.next_sync_idx[cur] = self._role.confirm_sync_idx[cur] + 1
            # Leader已经接收来自客户端的操作 尝试Append Entries RPC同步给集群Follower
            self.start_heartbeat()
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
        if not isinstance(self._role, Leader):
            logger.info(f'当前{self._role}不是Leader 无权处理客户端的put指令')
            return RPC(direction=RPC_Direction.RESP, type=RPC_Type.CLIENT_PUT_FORWARD, content=PutDataResp(succ=False).json())
        # Leader直接存到logs里面 等待下一轮的Append Entries同步给Leader
        self._role.update_log(Entry(index=len(self._role.logs), term=self._role.cur_term, key=req.key, value=req.val))
        # 给Leader自己添加了日志量
        cur: Address = self._id()
        sync_cnt: int = 1
        self._peers_sent[cur] = sync_cnt
        prev_log_idx: int = self._role.next_sync_idx[cur]-1
        # 更新同步确认的点位
        self._role.confirm_sync_idx[cur] = max(self._role.confirm_sync_idx[cur], prev_log_idx+sync_cnt)
        self._role.next_sync_idx[cur] = self._role.confirm_sync_idx[cur] + 1
        # 日志已经持久化 触发一次Append Entries RPC
        self.start_heartbeat()
        return RPC(direction=RPC_Direction.RESP, type=RPC_Type.CLIENT_PUT_FORWARD, content=PutDataResp(succ=True).json())
