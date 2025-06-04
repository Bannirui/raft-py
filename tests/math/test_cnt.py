import unittest

class TestMyModule(unittest.TestCase):
    def test_01(self):
        nums: list[int] = [1, 2, 3, 4, 5]
        threshold_num: int = 2
        ret: int = sum(num >= threshold_num for num in nums)
        self.assertEqual(ret, 4)
    def test_02(self):
        nums: list[int] = [1, 2, 3, 4, 5]
        threshold_num: int = 2
        ret: int = sum(num > threshold_num for num in nums)
        self.assertEqual(ret, 3)

if __name__ == "__main__":
    unittest.main()
