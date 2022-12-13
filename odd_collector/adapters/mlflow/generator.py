from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostSettings


class MlflowPathsModel(BasePathsModel):
    pipelines: Optional[str]
    jobs: Optional[str]

    class Config:
        dependencies_map = {
            "experiments": ("experiments",),
            "jobs": ("experiments", "jobs"),
        }


class MlFlowGenerator(Generator):
    source = "mlflow"
    server_model = HostSettings
    paths_model = MlflowPathsModel
