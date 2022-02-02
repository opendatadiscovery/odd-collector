import pydantic

from typing import List
from odd_collector.domain.plugin import AvailablePlugin


class CollectorConfig(pydantic.BaseModel):
    default_pulling_interval: int
    token: str
    plugins: List[AvailablePlugin]

    class Config:
        smart_union = True