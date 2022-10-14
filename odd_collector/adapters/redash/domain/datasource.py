from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class DataSource:
    name: str
    syntax: str
    options: Dict[str, Any]
    type: str
    id: int
