from argparse import ArgumentParser

# 集群服务器配置
from conf import cluster_servers

parser = ArgumentParser(description="Raft Server")
parser.add_argument(
    "--MyId",
    type=int,
    required=True,
    help="server id",
)
args = parser.parse_args()

def main()->None:
    """
    启动参数--MyId指定当前实例作为集群中的节点id
    在配置文件中指定集群启动节点配置{id, ip, port}
    """
    print(f"当前节点id={args.MyId}")
    [print(f"集群服务器配置={ser}") for ser in cluster_servers]

if __name__ == '__main__':
    main()