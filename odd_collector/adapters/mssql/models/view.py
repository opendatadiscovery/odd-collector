from dataclasses import dataclass, field
from typing import Dict, List

from odd_collector.adapters.mssql.models.column import Column


@dataclass
class ViewDependency:
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str


@dataclass
class View:
    view_catalog: str
    view_schema: str
    view_name: str
    view_dependencies: List[ViewDependency]
    columns: List[Column] = field(default_factory=list)

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            "catalog": self.view_catalog,
            "schema": self.view_schema,
            "name": self.view_name,
            "depends_on": ", ".join(dep.table_name for dep in self.view_dependencies),
        }
