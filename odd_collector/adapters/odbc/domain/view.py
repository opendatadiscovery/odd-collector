from dataclasses import dataclass

from odd_collector.adapters.odbc.domain.base_table import BaseTable


@dataclass
class View(BaseTable):
    ...
