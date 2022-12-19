from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from mlflow.entities.model_registry import ModelVersion as MLFlowModelVersion
from odd_collector.helpers.datetime_from_ms import datetime_from_milliseconds

@dataclass
class ModelVersion:
    name: str
    version: int
    created_at: datetime
    updated_at: Optional[datetime]
    description: Optional[str]
    user_id: Optional[int]
    current_stage: Optional[str]
    source: Optional[str]
    run_id: Optional[str]
    run_link: Optional[str]
    status: Optional[str]
    status_message: Optional[str]
    tags: Dict[str, Any]

    @classmethod
    def from_mlflow(cls, mv: MLFlowModelVersion) -> "ModelVersion":
        return cls(
            name=mv.name,
            version=mv.version,
            created_at= datetime_from_milliseconds(mv.creation_timestamp),
            updated_at=datetime_from_milliseconds(mv.last_updated_timestamp),
            description=mv.description,
            user_id=mv.user_id,
            current_stage=mv.current_stage,
            source=mv.source,
            run_id=mv.run_id,
            run_link=mv.run_link,
            status=mv.status,
            status_message=mv.status_message,
            tags={tag.key: tag.value for tag in mv.tags or []}
        )

    @property
    def full_name(self):
        return f"{self.name}:{self.version}"

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            'current_stage': self.current_stage,
            'source': self.source,
            'run_id': self.run_id,
            'run_link': self.run_link,
            'status': self.status,
            'status_message': self.status_message,
            **self.tags
        }
