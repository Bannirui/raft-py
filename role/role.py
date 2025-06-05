from __future__ import annotations

from pydantic import BaseModel, NonNegativeInt

from db import Entry, DatabaseDriver
from log import logger
from util import Address


class Role(BaseModel):
    _driver: DatabaseDriver = DatabaseDriver()
    # 初始化时保证0-based 默认值0
    cur_term: int = _driver.get_current_term()
    # 缓存的所有的日志条目
    logs: list[Entry] = _driver.get_log()
    """
    Leader会通过Append Entry的RPC调用把哪些entry已经被集群大多数节点确认并可以提交的这个index信息告诉Follower
    追加日志的(last_commit_idx...commit_idx]都是待提交的
    """
    pending_commit_idx_threshold: int = -1
    # 已经提交的日志 [0...last_commit_idx]都是已经提交的
    last_commit_idx: int = -1
    voted_for: Address | None = _driver.get_voted_for()

    def update_cur_term(self, term: int) -> None:
        """
        更新term
        Args:
            term 要更新成的term
        """
        self.cur_term = self._driver.set_current_term(term)

    def update_voted_for(self, voted_for: Address | None) -> None:
        """
        选举投票
        Args:
          target 投票给谁
        """
        self.voted_for = self._driver.set_voted_for(voted_for)
        if voted_for is not None:
            logger.info(f'{self}当前任期{self.cur_term}投票给{voted_for}')

    def update_log(self, new_entry: Entry) -> None:
        logger.info(f'开始执行日志持久化 需要存储的entry是{new_entry}')
        self.logs = self._driver.set_log(new_entry)

    def commit(self):
        """尝试提交日志"""
        logger.info(f'{self}开始执行提交')
        # 待提交的日志(last...threshold]
        while self.pending_commit_idx_threshold > self.last_commit_idx:
            entry = self.logs[self.last_commit_idx+1]
            logger.info(f'将{entry}持久化到db')
            self._driver.set_db(entry.key, entry.value)
            self.last_commit_idx += 1
        logger.info(f'{self}结束提交')
    def __str__(self) -> str:
        return f'待提交区间({self.last_commit_idx}...{self.pending_commit_idx_threshold}]'

class Candidate(Role):
    def __str__(self):
        return f'Candidate{super().__str__()}'


class Follower(Role):
    def __str__(self):
        return f'Follower{super().__str__()}'


class Leader(Role):
    # Leader需要发起同步的日志 Leader维护待同步给Follower的index 初始时指向logs尾=len(logs)
    # [...idx-1]都是已经需要同步的 idx就是准备同步的
    next_sync_idx: dict[Address, int]
    # 已经确认同步的日志 Leader已经同步给Follower的index 收到了来自Follower的回复 初始时-1
    # [...idx]都是已经同步好的
    confirm_sync_idx: dict[Address, int]

    def __str__(self):
        return f'Leader{super().__str__()}'
