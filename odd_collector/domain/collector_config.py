import pydantic

from typing import List, Union
from odd_collector.domain.plugin import Plugin, DynamoDbPlugin, GluePlugin, AthenaPlugin, S3Plugin


class CollectorConfig(pydantic.BaseModel):
    default_pulling_interval: int
    token: str
    plugins: List[Union[DynamoDbPlugin, GluePlugin, AthenaPlugin, S3Plugin]] = pydantic.Field(...)

    class Config:
        smart_union = True
