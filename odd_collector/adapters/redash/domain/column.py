from dataclasses import dataclass
from typing import Optional


@dataclass
class Column:
    name: str
    remote_type: Optional[str]
