import unittest

import json

from typing import Optional

from util import FrozenModel


class _CaptureTerm(FrozenModel):
    # 可选字段 防止反序列化时失败报错
    term: Optional[int] = None


def deserial(s: str)->None:
    ret = _CaptureTerm.parse_raw(s)
    print("反序列化 ", ret)


class TestMyModule(unittest.TestCase):
    def test_01(self):
        data = {"name": "dingrui"}
        self.assertEqual(deserial(json.dumps(data)), 4)
    def test_02(self):
        data = {"name": "dingrui", "term": 2}
        self.assertEqual(deserial(json.dumps(data)), 4)


if __name__ == "__main__":
    unittest.main()
