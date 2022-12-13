from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .column import Column


@dataclass
class Table:
    name: str
    columns: List[Column]
    description: Optional[str]

    @property
    def metadata(self) -> Dict[str, Any]:
        return {}
