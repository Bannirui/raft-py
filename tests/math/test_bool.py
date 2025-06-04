import unittest


class TestMyModule(unittest.TestCase):
    def test_01(self):
        num: int = 0
        if num: num+=1
        self.assertEqual(num, 0)
    def test_02(self):
        num: int = None
        if num: num+=1
        self.assertEqual(num, 0)

if __name__ == "__main__":
    unittest.main()
