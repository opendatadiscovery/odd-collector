from dataclasses import dataclass
from typing import Any


@dataclass
class DestinationMetadata:
    id: Any
    group_id: Any
    service: Any
    region: Any
    time_zone_offset: Any
    setup_status: Any
    config: Any
