from dataclasses import dataclass
from typing import Dict


@dataclass
class Table:
    table_name: str
    table_catalog: str
    table_schema: str
    table_type: str

    @property
    def metadata(self) -> Dict[str, str]:
        return self.__dict__
