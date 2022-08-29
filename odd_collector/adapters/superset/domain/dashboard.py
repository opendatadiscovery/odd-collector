from typing import Optional, Set


class Dashboard:
    def __init__(self,
                 id: int,
                 name: str,
                 datasets_ids: Optional[Set[int]] = None
                 ):
        self.datasets_ids = datasets_ids
        self.name = name
        self.id = id
