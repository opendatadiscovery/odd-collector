from dataclasses import dataclass, field
from typing import Any, Optional, Set


@dataclass
class Dashboard:
    id: int
    dashboard_title: str
    charts: Optional[Set[int]] = field(default_factory=set)
    # metadata: Optional[list[MetadataExtension]] = None
    # datasets_ids: Optional[Set[int]] = None
    # owner: Optional[str] = None
    # description: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]):
        return cls(
            id=data["id"],
            dashboard_title=data["dashboard_title"],
        )
