from __future__ import annotations

from pydantic import BaseModel, NonNegativeInt

from db import Entry, DatabaseDriver
from log import logger
from util import Address


class Role(BaseModel):
    _driver: DatabaseDriver = DatabaseDriver()
    cur_term: int = _driver.get_current_term()
    commit_idx: NonNegativeInt
    last_idx: NonNegativeInt
    voted_for: Address | None = _driver.get_voted_for()
    log: list[Entry] = _driver.get_log()

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
        self.log = self._driver.set_log(new_entry)

    def commit(self):
        logger.info(f'{self}开始执行提交 commit_idx={self.commit_idx} last_idx={self.last_idx}')
        while self.commit_idx > self.last_idx:
            entry = self.log[self.last_idx]
            self._driver.set_db(entry.key, entry.value)
            self.last_idx += 1


class Candidate(Role):
    """candidate"""

    def __str__(self):
        return 'Candidate'


class Follower(Role):
    """follower"""

    def __str__(self):
        return 'Follower'


class Leader(Role):
    """leader"""
    next_idx: dict[Address, int]
    match_idx: dict[Address, int]

    def __str__(self):
        return 'Leader'
