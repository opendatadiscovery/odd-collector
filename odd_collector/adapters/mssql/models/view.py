from dataclasses import dataclass, field
from typing import List

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
    columns: list[Column] = field(default_factory=list)

    @property
    def odd_metadata(self) -> dict[str, str]:
        return {}
