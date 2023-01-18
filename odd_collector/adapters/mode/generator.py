from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class ModePathModel(BasePathsModel):
    # TODO: clarify this class
    reports: str = ""

    class Config:
        dependencies_map = {"reports": ("reports",)}


class ModeGenerator(Generator):
    source = "mode"
    paths_model = ModePathModel
    server_model = HostnameModel
