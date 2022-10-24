from typing import List, Optional, Set

from odd_models.models import MetadataExtension


class Dashboard:
    def __init__(
        self,
        id: int,
        name: str,
        metadata: Optional[List[MetadataExtension]] = None,
        datasets_ids: Optional[Set[int]] = None,
        owner: Optional[str] = None,
        description: Optional[str] = None,
    ):
        self.description = description
        self.owner = owner
        self.metadata = metadata
        self.datasets_ids = datasets_ids
        self.name = name
        self.id = id
