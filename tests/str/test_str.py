import unittest

class _MyBase:
    def __init__(self, name: str)->None:
        self.name=name
    def __str__(self):
        return f'name={self.name}'
class _MyEntity(_MyBase):
    def __init__(self, name: str, age: int):
        super().__init__(name)
        self.age = age
    def __str__(self):
        return f'{super().__str__()} age={self.age}'

class TestMyModule(unittest.TestCase):
    def test_01(self):
        obj = _MyEntity("a", 1)
        print(obj)

if __name__ == "__main__":
    unittest.main()
