from pathlib import Path

from .config import ConfigLoader, ServerConfig

config_path: str = Path(__file__).parent.parent/"conf.toml"
assert config_path.exists(), f"项目配置文件不存在:{config_path}"
assert config_path.is_file(), f"{config_path}不是一个文件"

# 项目全局配置
app_cfg = ConfigLoader(config_path).get()