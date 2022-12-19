from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from mlflow.entities.model_registry import RegisteredModel

from ..domain.model_version import ModelVersion
from odd_collector.helpers.datetime_from_ms import datetime_from_milliseconds

@dataclass
class Model:
    name: str
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    description: Optional[str]
    tags: Dict[str, Any]
    model_versions: Optional[List[ModelVersion]] = field(default_factory=list)

    @classmethod
    def from_mlflow(cls, model: RegisteredModel) -> "Model":
        return cls(
            name=model.name,
            created_at= datetime_from_milliseconds(model.creation_timestamp),
            updated_at=datetime_from_milliseconds(model.last_updated_timestamp),
            description=model.description,
            tags={tag.key: tag.value for tag in model.tags or []}
        )

    @property
    def metadata(self) -> Dict[str, str]:
        return {**self.tags}
