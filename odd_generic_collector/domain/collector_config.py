import pydantic

from typing import List
from odd_generic_collector.domain.plugin import AvailablePlugin


class CollectorConfig(pydantic.BaseModel):
    default_pulling_interval: int
    token: str
    plugins: List[AvailablePlugin]
    platform_host_url: str

    class Config:
        smart_union = True
