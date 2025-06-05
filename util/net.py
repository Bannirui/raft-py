"""网络开发相关类型定义"""
from typing import Annotated
from pydantic import Field

from util.model import FrozenModel

REGEX_HOSTNAME = r"(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])"
REGEX_IP_ADDRESS = r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])"
REGEX_SOCKET_ADDRESS = f"^({REGEX_HOSTNAME}|{REGEX_IP_ADDRESS})$"

_Host = Annotated[str, Field(pattern=REGEX_SOCKET_ADDRESS)]
_Port = Annotated[int, Field(ge=0, le=1<<16)]

class Address(FrozenModel):
    host: _Host
    port: _Port

    def __str__(self)->str:
        return f'{self.host}:{self.port}'