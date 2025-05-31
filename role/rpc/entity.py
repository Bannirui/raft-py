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
    """
    Leader的核心功能就是使用RPC中的Append Entries方式向Follower发送entry
    作用有3个
    1 心跳heartbeat---如果没有新的entry Leader也会定期发送空的AppendEntries来维持自己的领导地位
    2 复制日志entry---把客户端请求封装的entry发送出去 等待Follower追加
    3 提交日志commit---告诉Follower哪些entry已经被集群大多数节点确认并可以提交到状态机
    """
    term: NonNegativeInt
    leader_identity: Address
    prev_log_idx: NonNegativeInt
    prev_log_term: NonNegativeInt
    entries: list[Entry]
    # Leader告诉Follower哪些entry已经被大多数节点确认可以提交 追加日志的[0...leader_commit_idx]都可以提交了
    leader_commit_idx: NonNegativeInt

class AppendEntryResp(BaseRPC):
    term: NonNegativeInt
    succ: StrictBool