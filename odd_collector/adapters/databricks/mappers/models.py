from dataclasses import dataclass, field
from typing import Any

from odd_collector.models import Table, Column


@dataclass(frozen=True)
class DatabricksColumn(Column):
    odd_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass()
class DatabricksTable(Table):
    columns: list[DatabricksColumn] = field(default_factory=list)
    odd_metadata: dict[str, Any] = field(default_factory=dict)
