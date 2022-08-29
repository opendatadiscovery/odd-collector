from typing import Optional
from oddrn_generator.generators import Generator, HostnameModel
from oddrn_generator.path_models import BasePathsModel


class SupersetPathsModel(BasePathsModel):
    databases: Optional[str]
    datasets: Optional[str]
    columns: Optional[str]
    dashboards: Optional[str]

    class Config:
        dependencies_map = {
            "databases": (
                "databases",
            ),
            "datasets": ("databases", "datasets"),
            "columns": ("databases", "datasets", "columns"),
            "dashboards": ("dashboards", )
        }
        # data_source_path = "databases"


class SupersetGenerator(Generator):
    source = "superset"
    paths_model = SupersetPathsModel
    server_model = HostnameModel
