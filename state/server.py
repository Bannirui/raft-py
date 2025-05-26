from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR
from random import uniform
from time import time
from select import select

from conf import ServerConfig

# 超时区间[5...8]s
TIMEOUT_SECOND_L: float = 5
TIMEOUT_SECOND_H: float = 8

class Server:
    # 当前节点id
    id: int
    # 集群中各个节点的配置
    peers: list[ServerConfig]
    # 当前节点的配置
    cfg: ServerConfig
    sock: socket | None = None
    # 超时到期时间
    timeout: float = time() + uniform(TIMEOUT_SECOND_L, TIMEOUT_SECOND_H)

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
        # todo
        pass

    def start_heartbeat(self) -> None:
        # todo
        pass

    def start_election(self) -> None:
        # todo
        pass

    def rpc_handle(self) -> None:
        # todo
        pass

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
                    if self.is_leader:
                        self.start_heartbeat()
                    else:
                        self.start_election()
                
                for s in readable:
                    data, addr = s.recvfrom(1024)
                    for payload in data.decode().splitlines(keepends=True):
                        self.rpc_handle()

                1/0
        except KeyboardInterrupt:
            print("服务正常退出")
        except Exception as e:
            print(f"服务执行异常{e}")