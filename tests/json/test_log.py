import os
import json
from pathlib import Path
import unittest

from pydantic import BaseModel


class _Person(BaseModel):
    name: str
    age: int

class _People(BaseModel):
    data: list[_Person]

relative = lambda path: Path(__file__).parent / path

def guard_file(path: str, default: dict)->None:
    if os.path.exists(path): return
    # 创建文件
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # 写入默认内容
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(default, f)

guard_file(relative('json/log.json'), _People(data=[]).model_dump())

class TestMyModule(unittest.TestCase):
    def test_01(self):
        data = _People.parse_file(relative('json/log.json'))
        if not data:
            print('不存在')
        else: print(f'len={len(data.data)}')
        print(f'data={data}')

if __name__ == "__main__":
    unittest.main()
