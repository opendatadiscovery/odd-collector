from typing import Optional

from oddrn_generator.generators import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class MongoPathsModel(BasePathsModel):
    databases: str
    collections: Optional[str]
    columns: Optional[str]

    class Config:
        dependencies_map = {
            "databases": ("databases",),
            "collections": ("databases", "collections"),
            "columns": ("databases", "collections", "columns"),
        }
        data_source_path = "databases"


class MongoGenerator(Generator):
    source = "mongo"
    paths_model = MongoPathsModel
    server_model = HostnameModel
