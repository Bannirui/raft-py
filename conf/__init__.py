from pathlib import Path

from .config import ConfigLoader, ServerConfig, ProjConfig

config_path: Path = Path(__file__).parent.parent/"conf.toml"
assert config_path.exists(), f"项目配置文件不存在:{config_path}"
assert config_path.is_file(), f"{config_path}不是一个文件"

# 项目全局配置
app_cfg: ProjConfig = ConfigLoader(config_path).get()
assert app_cfg, '项目配置不能为空'
assert app_cfg.my_id, 'my_id不能为空'
assert app_cfg.servers, '集群节点配置不能为空'