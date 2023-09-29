from dataclasses import dataclass
from typing import Optional


@dataclass
class Column:
    id: str
    name: str
    is_nullable: bool
    remote_type: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_dict(cls, **data) -> "Column":
        return cls(
            data["id"],
            data["name"],
            data["isNullable"],
            data.get("remoteType"),
            data.get("description"),
        )
