from typing import Optional

from typing_extensions import Literal
from enum import IntEnum

from .model import FrozenModel

class RPC_Direction(IntEnum):
    REQ = 1
    RESP = 1 << 1
class RPC_Type(IntEnum):
    # raft的核心 leader通过append entries方式向follower发送entry
    APPEND_ENTRIES = 1
    # 拉票请求
    REQUEST_VOTE = 1<<1
    ADD_SERVER = 1<<2
    REMOVE_SERVER = 1<<3
    INSTALL_SNAPSHOT = 1<<4
    REGISTER_CLIENT = 1<<5
    # 客户端存数据
    CLIENT_PUT = 1 << 6
    # 转发的客户端存数据请求
    CLIENT_PUT_FORWARD = 1 << 7
    # 客户端取数据
    CLIENT_QUERY = 1<<8
class RPC(FrozenModel):
    direction: Literal[RPC_Direction.REQ, RPC_Direction.RESP]
    type: Literal[
        RPC_Type.APPEND_ENTRIES,
        RPC_Type.REQUEST_VOTE,
        RPC_Type.ADD_SERVER,
        RPC_Type.REMOVE_SERVER,
        RPC_Type.INSTALL_SNAPSHOT,
        RPC_Type.REGISTER_CLIENT,
        RPC_Type.CLIENT_PUT,
        RPC_Type.CLIENT_PUT_FORWARD,
        RPC_Type.CLIENT_QUERY,
    ]
    content: Optional[str] = None