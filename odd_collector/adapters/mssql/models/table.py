from dataclasses import dataclass, field
from typing import Dict, List

from funcy import omit

from odd_collector.adapters.mssql.models.column import Column


@dataclass
class Table:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str
    columns: List[Column] = field(default_factory=list)

    @property
    def metadata(self) -> Dict[str, str]:
        return omit(self.__dict__, ["columns"])
