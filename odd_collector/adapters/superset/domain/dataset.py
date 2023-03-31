from typing import List, Optional

from odd_models.models import MetadataExtension

from odd_collector.adapters.superset.domain.column import Column


class Dataset:
    def __init__(
        self,
        id: int,
        name: str,
        db_id: int,
        db_name: str,
        kind: str,
        schema: str,
        columns: List[Column] = None,
        metadata: Optional[List[MetadataExtension]] = None,
        owner: Optional[str] = None,
        description: Optional[str] = None,
    ):
        self.schema = schema
        self.description = description
        self.owner = owner
        self.metadata = metadata
        self.id = id
        self.name = name
        self.database_name = db_name
        self.database_id = db_id
        self.kind = kind
        self.columns = columns or []

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            databases=self.database_name, datasets=self.name
        )
        return oddrn_generator.get_oddrn_by_path("datasets")
