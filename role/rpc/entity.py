from pydantic import NonNegativeInt, StrictBool

from util import Address
from util.model import FrozenModel
from db import Entry

class BaseRPC(FrozenModel):
    pass

class VoteReq(BaseRPC):
    """投票选举req"""
    term: NonNegativeInt
    candidate_identity: Address
    last_log_idx: NonNegativeInt
    last_log_term: NonNegativeInt

class VoteResp(BaseRPC):
    """投票选择resp 对别人的拉票作回复"""
    term: NonNegativeInt
    # 同不同意投票 True做出投票 False不同意投票
    vote_granted: StrictBool

class AppendEntryReq(BaseRPC):
    term: NonNegativeInt
    leader_identity: Address
    prev_log_idx: NonNegativeInt
    prev_log_term: NonNegativeInt
    entries: list[Entry]
    leader_commit_idx: NonNegativeInt

class AppendEntryResp(BaseRPC):
    term: NonNegativeInt
    succ: StrictBool