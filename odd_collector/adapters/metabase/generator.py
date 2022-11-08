from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class MetabasePathModel(BasePathsModel):
    collections: str = ""
    dashboards: Optional[str]
    cards: Optional[str]

    class Config:
        dependencies_map = {
            "collections": ("collections",),
            "dashboards": (
                "collections",
                "dashboards",
            ),
            "cards": (
                "collections",
                "cards",
            ),
        }


class MetabaseGenerator(Generator):
    source = "metabase"
    paths_model = MetabasePathModel
    server_model = HostnameModel
