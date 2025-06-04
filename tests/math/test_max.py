import unittest


class TestMyModule(unittest.TestCase):
    def test_01(self):
        nums: list[int] = [-1, -1, -1]
        ret: int = max(nums)
        self.assertEqual(ret, -1)
        ans: int = max(ret, 10)
        self.assertEqual(ans, 10)

if __name__ == "__main__":
    unittest.main()
