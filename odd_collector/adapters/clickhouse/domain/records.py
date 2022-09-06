from dataclasses import dataclass
from typing import List

from .column import Column
from .integration_engine import IntegrationEngine
from .table import Table


@dataclass
class Records:
    columns: List[Column]
    tables: List[Table]
    integration_engines: List[IntegrationEngine]
