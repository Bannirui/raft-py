from __future__ import annotations
from pydantic import BaseModel, NonNegativeInt

from util import Address
from conf import ServerConfig

class Role(BaseModel):
    cur_term: NonNegativeInt = 0
    commit_idx: NonNegativeInt
    last_idx: NonNegativeInt
    voted_for: Address | None = None
    def update_cur_term(self, term: int) -> None:
        """
        更新term
        Args:
            term 要更新成的term
        """
        self.cur_term = term
    def vote_for(self, target: Address) -> None:
        """
        选举投票
        Args:
          target 投票给谁
        """
        self.voted_for = target
    
class Candidator(Role):
    """candidator"""

class Follower(Role):
    """follower"""

class Leader(Role):
    """leader"""
    next_idx: dict[ServerConfig, NonNegativeInt]
    match_idx: dict[ServerConfig, NonNegativeInt]