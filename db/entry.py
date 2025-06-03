from pydantic import BaseModel, NonNegativeInt, StrictStr

class Entry(BaseModel):
    # Append Entries中的索引脚标
    index: int
    # Leader和term
    term: int
    # 要存取的数据
    key: str
    value: str