import pydantic

from typing import List, Union
from odd_collector.domain.plugin import Plugin, DynamoDbPlugin, GluePlugin, AthenaPlugin


class CollectorConfig(pydantic.BaseModel):
    default_pulling_interval: int
    token: str
    plugins: List[Union[DynamoDbPlugin, GluePlugin, AthenaPlugin]] = pydantic.Field(...)

    class Config:
        smart_union = True
