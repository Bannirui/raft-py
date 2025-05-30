from pydantic import BaseModel
import tomllib

class ServerConfig(BaseModel):
    """
    集群中每个节点的配置
    """
    id: int
    ip: str
    port: int

class ProjConfig(BaseModel):
    """
    项目配置
    """
    # 集群节点服务器配置
    servers: list[ServerConfig]
    # 集群节点超时
    timeout_second_lo: float
    timeout_second_hi: float

class ConfigLoader:
    """
    配置加载器 用于从TOML文件中加载配置信息并封装成结构化对象
    """

    def __init__(self, path:str) -> None:
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