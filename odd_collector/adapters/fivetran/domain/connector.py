from dataclasses import dataclass
from typing import Any


@dataclass
class ConnectorMetadata:
    id: Any
    group_id: Any
    service: Any
    service_version: Any
    schema: Any
    connected_by: Any
    created_at: Any
    succeeded_at: Any
    failed_at: Any
    paused: Any
    pause_after_trial: Any
    sync_frequency: Any
    schedule_type: Any
    status: Any
    config: Any
