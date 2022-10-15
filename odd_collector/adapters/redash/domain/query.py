from dataclasses import dataclass
from typing import Optional, List, Dict, Any


@dataclass
class Query:
    id: int
    query: str
    data_source_id: str
    name: str
    columns: Optional[List[str]] = None

    @staticmethod
    def from_response(node: Dict[str, Any]):
        return Query(id=node['id'],
                     query=node['query'],
                     data_source_id=node['data_source_id'],
                     name=node['name']
                     )

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            queries=self.name
        )
        return oddrn_generator.get_oddrn_by_path("queries")
