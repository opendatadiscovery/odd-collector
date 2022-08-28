from typing import List
from odd_collector.adapters.superset.domain.column import Column


class Dataset:
    def __init__(
            self,
            id: str,
            name: str,
            db_id: str,
            db_name: str,
            columns: List[Column] = None,
    ):
        self.id = id
        self.name = name
        self.database_name = db_name
        self.database_id = db_id
        self.columns = columns or []

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            databases=self.database_name,
            datasets=self.name,
        )
        return oddrn_generator.get_oddrn_by_path("datasets")
