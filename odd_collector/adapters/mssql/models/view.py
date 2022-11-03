from dataclasses import dataclass
from typing import Dict, List


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

    @property
    def metadata(self) -> Dict[str, str]:
        return {
            "catalog": self.view_catalog,
            "schema": self.view_schema,
            "name": self.view_name,
            "depends_on": ", ".join(dep.table_name for dep in self.view_dependencies),
        }
