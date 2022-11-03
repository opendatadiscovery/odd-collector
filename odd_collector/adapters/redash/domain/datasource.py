from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class DataSource:
    name: str
    syntax: str
    options: Dict[str, Any]
    type: str
    id: int

    @staticmethod
    def from_response(node: Dict[str, Any]):
        return DataSource(
            name=node["name"],
            syntax=node["syntax"],
            options=node["options"],
            type=node["type"],
            id=node["id"],
        )
