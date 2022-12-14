from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class MlflowPathsModel(BasePathsModel):
    experiments: Optional[str]
    jobs: Optional[str]

    class Config:
        dependencies_map = {
            "experiments": ("experiments",),
            "jobs": ("experiments", "jobs"),
        }


class MlFlowGenerator(Generator):
    source = "mlflow"
    server_model = HostnameModel
    paths_model = MlflowPathsModel
