from abc import ABC
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from odd_collector.adapters.odbc.domain.column import Column


@dataclass
class BaseTable(ABC):
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str
    remarks: str
    columns: Optional[List[Column]] = field(default_factory=list)

    @property
    def metadata(self) -> Dict[str, Any]:
        return {"remarks": self.remarks}

    @classmethod
    def from_response(cls, response: Tuple[Any]):
        return cls(*response)
