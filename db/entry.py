from pydantic import BaseModel, NonNegativeInt, StrictStr

class Entry(BaseModel):
    index: NonNegativeInt
    term: NonNegativeInt
    key: StrictStr
    value: StrictStr