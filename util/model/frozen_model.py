from pydantic import BaseModel, ConfigDict
from orjson import dumps, loads

class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)
    def model_dump_json_custom(self) -> str:
        return dumps(self.model_dump()).decode()
    @classmethod
    def model_validate_json_custom(cls, json_str: str):
        return cls.model_validate(loads(json_str))