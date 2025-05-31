from typing_extensions import Literal
from enum import IntEnum

from .model import FrozenModel

class RPC_Direction(IntEnum):
    REQUEST = 1
    RESPONSE = 1<<1
class RPC_Type(IntEnum):
    APPEND_ENTRIES = 1
    # 拉票请求
    REQUEST_VOTE = 1<<1
    ADD_SERVER = 1<<2
    REMOVE_SERVER = 1<<3
    INSTALL_SNAPSHOT = 1<<4
    REGISTER_CLIENT = 1<<5
    CLIENT_REQUEST = 1<<6
    CLIENT_QUERY = 1<<7
class RPC(FrozenModel):
    direction: Literal[RPC_Direction.REQUEST, RPC_Direction.RESPONSE]
    type: Literal[
        RPC_Type.APPEND_ENTRIES,
        RPC_Type.REQUEST_VOTE,
        RPC_Type.ADD_SERVER,
        RPC_Type.REMOVE_SERVER,
        RPC_Type.INSTALL_SNAPSHOT,
        RPC_Type.REGISTER_CLIENT,
        RPC_Type.CLIENT_REQUEST,
        RPC_Type.CLIENT_QUERY,
    ]
    content: str