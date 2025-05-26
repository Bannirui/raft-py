from argparse import ArgumentParser

from conf import app_cfg, ServerConfig
from state import Server
from role import *

parser = ArgumentParser(description = "Raft Server")
parser.add_argument(
    "--MyId",
    type=int,
    required=True,
    help="server id",
)
args = parser.parse_args()

def main() -> None:
    """
    启动参数--MyId指定当前实例作为集群中的节点id
    在配置文件中指定集群启动节点配置{id, ip, port}
    """
    # 当前节点
    my_id: int = args.MyId
    servers: list[ServerConfig] = app_cfg.servers
    assert servers, f"集群服务器配置不能为空"
    # 集群配置
    cluster_server_map: dict = {ser.id:ser for ser in servers}
    # 启动的id
    my_conf: ServerConfig | None = None
    if (my_conf := cluster_server_map.get(my_id)) is None:
        raise ValueError(f"集群配置是{servers} 当前启动的MyId为{my_id}是无效的")
    with Server(id=my_conf.id, peers=servers) as server:
        server.start()
    print("服务退出")

if __name__ == '__main__':
    main()