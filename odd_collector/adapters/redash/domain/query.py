from dataclasses import dataclass
from typing import Optional, List


@dataclass
class Query:
    id: int
    query: str
    data_source_id: str
    name: str
    columns: Optional[List[str]] = None

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            queries=self.name
        )
        return oddrn_generator.get_oddrn_by_path("queries")
