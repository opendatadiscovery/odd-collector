import dataclasses
from typing import Any


@dataclasses.dataclass
class IntegrationEngine:
    table: str
    name: str
    settings: Any
