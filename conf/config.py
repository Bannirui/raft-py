from pathlib import Path
from pydantic import BaseModel
import tomllib

class ServerConfig(BaseModel):
    """
    集群中每个节点的配置
    """
    # 节点标识
    id: int
    # 节点启动的ip
    ip: str
    # 集群节点间通信端口
    port: int
    # 集群对外服务端口
    data_port: int

class ProjConfig(BaseModel):
    """
    项目配置
    """
    # 集群节点服务器配置
    servers: list[ServerConfig]
    # 集群节点超时
    timeout_second_lo: float
    timeout_second_hi: float
    # 节点标识
    my_id: int

class ConfigLoader:
    """
    配置加载器 用于从TOML文件中加载配置信息并封装成结构化对象
    """

    def __init__(self, path: Path | str) -> None:
        """
        构造方法

        Args:
            path(str) 配置文件的绝对路径(.toml)
        """
        with open(path, "rb") as f:
            data = tomllib.load(f)
        self.config = ProjConfig(**data)

    def get(self)->ProjConfig:
        return self.config