import json
import os.path
from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel

from util.net import Address
from . import Entry


class _Database(BaseModel):
    db: dict[str, str]


class _Log(BaseModel):
    logs: list[Entry]


class _State(BaseModel):
    current_term: int
    voted_for: Address | None

# json文件的相对目录
relative = lambda path: Path(__file__).parent / path

def guard_file(path: str, default: dict)->None:
    """
    持久化文件可能在项目启动时候不存在 直接用pydantic加载会报错的

    Args:
        path: 文件路径 绝对路径
        default: 默认写在文件中的内容作初始化
    """
    if os.path.exists(path): return
    # 创建文件
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # 写入默认内容
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(default, f)

# 确保文件文件
guard_file(relative('json/db.json'), _Database(db={}).model_dump())
# 初始化时没有Entry日志
guard_file(relative('json/log.json'), _Log(logs=[]).model_dump())
# 初始化term=0
guard_file(relative('json/state.json'), _State(current_term=0, voted_for=None).model_dump())

class DatabaseDriver(BaseModel):
    """类需要继承BaseModel主要需要pydantic的序列化功能 但是下面这几个成员不作为模型字段 要通过ClassVar声明成类变量"""
    _db: ClassVar[_Database] = _Database.parse_file(relative('json/db.json'))
    _log: ClassVar[_Log] = _Log.parse_file(relative('json/log.json'))
    _state: ClassVar[_State] = _State.parse_file(relative('json/state.json'))

    @staticmethod
    def _dump(path: str, content: str) -> None:
        with open(relative(path), mode='w') as f:
            f.write(f'{content}\n')

    @classmethod
    def _dump_db(cls) -> None:
        cls._dump('json/db.json', cls._db.json())

    @classmethod
    def _dump_log(cls) -> None:
        cls._dump('json/log.json', cls._log.json())

    @classmethod
    def _dump_state(cls) -> None:
        cls._dump('json/state.json', cls._state.json())

    @classmethod
    def get_db(cls, key: str) -> str | None:
        return cls._db.db[key] if isinstance(key, str) else None

    @classmethod
    def get_current_term(cls) -> int:
        return cls._state.current_term

    @classmethod
    def get_voted_for(cls) -> Address | None:
        return cls._state.voted_for

    @classmethod
    def get_entry(cls, i: int) -> Entry | None:
        return cls._log.logs[i] if (isinstance(i, int) and 0 <= i < len(cls._log.logs)) else None

    @classmethod
    def get_log(cls) -> list[Entry]:
        return cls._log.logs

    @classmethod
    def last_index(cls) -> int:
        return len(cls._log.logs) - 1

    @classmethod
    def set_db(cls, key: str, value: str) -> bool:
        if isinstance(key, str) and isinstance(value, str):
            cls._db.db[key] = value
            return True
        else:
            return False

    @classmethod
    def set_current_term(cls, new_term: int) -> int:
        if isinstance(new_term, int) and new_term > cls._state.current_term:
            cls._state.current_term = new_term
            cls._state.voted_for = None
            cls._dump_state()
        return cls._state.current_term

    @classmethod
    def set_voted_for(cls, voted_for: Address | None) -> Address | None:
        if voted_for is None or isinstance(voted_for, Address):
            cls._state.voted_for = voted_for
            cls._dump_state()
        return cls._state.voted_for

    @classmethod
    def set_log(cls, new_entry: Entry) -> list[Entry]:
        # 有效的日志idx肯定是[0...len(logs)]
        if isinstance(new_entry, Entry) and 0 <= new_entry.index <= len(cls._log.logs):
            existing_entry: Entry = None
            if new_entry.index < len(cls._log.logs):
                existing_entry = cls._log.logs[new_entry.index]
            if existing_entry is None:
                cls._log.logs.append(new_entry)
                cls._dump_log()
            elif new_entry.term != existing_entry.term:
                cls._log.logs = cls._log.logs[:new_entry.index]
                cls._log.logs.append(new_entry)
                cls._dump_log()
        return cls._log.logs
