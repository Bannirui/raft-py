from pydantic import BaseModel, NonNegativeInt, StrictStr

class Entry(BaseModel):
    # Append Entries中的索引脚标 0-based
    index: int
    # Leader的term 0-based
    term: int
    # 要存取的数据
    key: str
    value: str