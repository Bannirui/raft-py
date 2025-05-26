from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR
from random import uniform
from time import time
from select import select
from pydantic import BaseModel, NonNegativeInt

from conf import ServerConfig
from role import Role, Candidator, Follower, Leader
from role.rpc import VoteReq
from util import Address, RPC, RPC_Direction, RPC_Type

# 超时区间[5...8]s
TIMEOUT_SECOND_LO: float = 1
TIMEOUT_SECOND_HI: float = 3

class Server:
    # 当前节点id
    id: int
    # 集群中各个节点的配置
    peers: list[ServerConfig]
    _peers_sent: dict[ServerConfig, NonNegativeInt]
    # 当前节点的配置
    cfg: ServerConfig
    sock: socket | None = None
    # 超时到期时间
    timeout: float = time() + uniform(TIMEOUT_SECOND_LO, TIMEOUT_SECOND_HI)
    # 当前节点角色类型
    _role: Role = Follower(commit_idx=0, last_idx=0)
    # 投票给了谁
    _votes: set[Address] = set()

    def __init__(self, id: int, peers: list[ServerConfig]):
        """
        构造方法
        """
        self.id = id
        self.peers = peers
        self.cfg = next((peer for peer in peers if peer.id == id), None)

    def __del__(self):
        pass

    def __enter__(self):
        self.open_sock()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_sock()

    def open_sock(self)-> None:
        """
        实例化socket
        """
        try:
            # udp
            self.sock = socket(AF_INET, SOCK_DGRAM)
            self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            self.sock.bind((self.cfg.ip, self.cfg.port))
        except Exception as e:
            print(f"实例化socket失败 {e}")
            exit(1)

    def close_sock(self)->None:
        print("关闭socket")
        if self.sock:
            self.sock.close

    def is_time_out(self) -> bool:
        """超时到期"""
        return time() > self.timeout

    def is_leader(self) -> bool:
        """角色类型"""
        return isinstance(self._role, Leader)

    def start_heartbeat(self) -> None:
        # 角色类型检查
        if not isinstance(self._role, Leader):
            return

    def start_election(self) -> None:
        """follower超时到了 参与竞选leader"""
        if self.sock is None:
            return
        self._role.update_cur_term(self._role.cur_term + 1)
        self._role_promote_to_candidate()
        self_sock_addr: Address = self._id()
        self._role.vote_for(self_sock_addr)
        self._votes = {self_sock_addr}
        self._timeout_reset()
        # 要socket发送的信息
        rpc: RPC = RPC(direction=RPC_Direction.REQUEST, type=RPC_Type.REQUEST_VOTE,
                       content=VoteReq(
                           term=self._role.cur_term,
                           candidate_identity=self_sock_addr,
                           last_log_idx=0,
                           last_log_term=0,
                       ).model_dump_json()
        )
        
        for peer in self.peers:
            # 自己给自己的投票已经在内存中处理过了 不需要走网路
            if peer.id == self.id: continue
            self._rpc_send(rpc, Address(host=peer.ip, port=peer.port))
    
    def _rpc_send(self, rpc: RPC, target: Address) -> None:
        """
        socket发送消息
        Args:
            rpc 发送什么消息
            target 发送给谁
        """
        if self.sock is None:
            print("socket没有初始化 没法发送消息")
        else:
            try:
                self.sock.sendto(f"{rpc.model_dump_json()}\n".encode(), (target.host, target.port))
            except Exception as e:
                print(f"socket发送失败{e}")

    def rpc_handle(self) -> None:
        # todo
        pass

    def commit(self) -> None:
        # todo
        pass

    def _timeout_reset(self, leader: bool = False) -> None:
        """重置但前节点的超时"""
        print("开始重置超时")
        duration: float = uniform(TIMEOUT_SECOND_LO, TIMEOUT_SECOND_HI)
        if leader:
            duration /= 3
        self.timeout = time() + duration

    def _role_promote_to_candidate(self) -> None:
        """follower->candidator"""
        self._role = Candidator(**self._role.model_dump())
    def _role_promote_to_leader(self) -> None:
        """candidator->leader"""
        print(f"candidator->leader, term is{self._role.last_idx}")
        self._role = Leader(**self._role.model_dump(),
                            next_idx={},
                            match_idx={}
                    )
        self._peers_sent = {conf:0 for conf in self.peers}

    def _id(self) -> Address:
        """当前节点的ip和port"""
        if self.sock is None:
            raise RuntimeError("当前节点的socket异常")
        host, port = self.sock.getsockname()
        return Address(host=host , port=port)

    def start(self) -> None:
        """
        核心逻辑
        """
        try:
            while True:
                print(f"还有{self.timeout-time():.2f}s到期")
            
                # io多路复用
                readable, _, exceptional = select([self.sock], [], [], max(0, self.timeout-time()))

                if self.is_time_out():
                    if self.is_leader():
                        self.start_heartbeat()
                    else:
                        self.start_election()
                
                for s in readable:
                    data, addr = s.recvfrom(1024)
                    for payload in data.decode().splitlines(keepends=True):
                        self.rpc_handle()

                # re-bind
                for s in exceptional:
                    if s is self.sock:
                        self.open_sock()

                self.commit()
        except KeyboardInterrupt:
            print("服务正常退出")
        except Exception as e:
            print(f"服务执行异常{e}")
            raise e