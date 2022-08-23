from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class CubeJsPathModel(BasePathsModel):
    cubes: str = ""

    class Config:
        dependencies_map = {"cubes": ("cubes",)}


class CubeJsGenerator(Generator):
    source = "cubejs"
    paths_model = CubeJsPathModel
    server_model = HostnameModel
