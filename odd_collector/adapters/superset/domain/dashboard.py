from typing import Optional, List


class Dashboard:
    def __init__(self,
                 id: str,
                 name: str,
                 datasets_ids: Optional[List[int]]
                 ):
        self.datasets_ids = datasets_ids
        self.name = name
        self.id = id
