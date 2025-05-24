from pathlib import Path
import tomllib

config_path:str=Path(__file__).parent.parent/"conf.toml"
assert config_path.exists(), f"项目配置文件不存在:{config_path}"
assert config_path.is_file(), f"{config_path}不是一个文件"

with open(config_path, "rb") as f:
    config=tomllib.load(f)

# 集群节点服务器配置
cluster_servers:list=config["servers"]