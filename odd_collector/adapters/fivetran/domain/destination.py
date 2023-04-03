from dataclasses import dataclass
from typing import Dict


@dataclass
class DestinationMetadata:
    id: str
    group_id: str
    service: str
    region: str
    time_zone_offset: str
    setup_status: str
    config: Dict[str, str]
