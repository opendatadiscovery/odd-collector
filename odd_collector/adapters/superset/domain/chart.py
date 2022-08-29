from typing import Dict


class Chart:
    def __init__(self,
                 id: int,
                 dataset_id: int,
                 dashboards_ids_names: Dict[int, str]

                 ):
        self.id = id
        self.dataset_id = dataset_id
        self.dashboards_ids_names = dashboards_ids_names
