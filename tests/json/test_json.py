import json
import unittest

class _Person:
    def __init__(self, name: str, age: int):
        self.name: str = name
        self.age: int = age
    def __str__(self):
        return f'Person(name={self.name} age={self.age})'

class _List:
    data: list[_Person]
    def __init__(self, data: list[_Person]):
        self.data = data

class TestMyModule(unittest.TestCase):
    def test_01(self):
        obj = _Person(name="dingrui", age=1)
        s: str = json.dumps(obj, default=lambda o: o.__dict__)
        print(s)
    def test_02(self):
        s: str = '{}'
        data = json.loads(s)
        obj = _Person(name=data.get("name", None), age=data.get("age", None))
        print(obj)
    def test_03(self):
        obj = _Person(name=None, age=None)
        s: str = json.dumps(obj, default=lambda o: o.__dict__)
        print(s)
    def test_04(self):
        s: str = '{"name": null, "age": null}'
        data = json.loads(s)
        obj = _Person(name=data.get("name", None), age=data.get("age", None))
        print(obj)
    def test_05(self):
        obj = _List(data=[])
        s: str = json.dumps(obj, default=lambda o: o.__dict__)
        data = json.loads(s)
        ret = _List(data=data.get("data", []))
        print(s)
        print(ret.data)

if __name__ == "__main__":
    unittest.main()
