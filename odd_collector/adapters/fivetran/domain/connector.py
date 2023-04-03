from dataclasses import dataclass
from datetime import datetime
from typing import Dict


@dataclass
class ConnectorMetadata:
    id: str
    group_id: str
    service: str
    service_version: int
    schema: str
    connected_by: str
    created_at: datetime
    succeeded_at: datetime
    failed_at: datetime
    paused: datetime
    pause_after_trial: bool
    sync_frequency: int
    schedule_type: str
    status: Dict[str, str]
    config: Dict[str, str]
