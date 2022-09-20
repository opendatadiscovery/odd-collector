import dataclasses
from typing import Any


@dataclasses.dataclass
class Table:
    catalog: str
    schema: str
    name: str
    type: str
    is_joinable: bool
    is_broadcast: bool
