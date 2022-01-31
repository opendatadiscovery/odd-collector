import pydantic

from typing import List, Union
from odd_collector.domain.plugin import DynamoDbPlugin, GluePlugin, Plugin


class CollectorConfig(pydantic.BaseModel):
    default_pulling_interval: str
    token: str
    plugins: List[Union[DynamoDbPlugin, GluePlugin]] = pydantic.Field(...)

    class Config:
        smart_union = True
