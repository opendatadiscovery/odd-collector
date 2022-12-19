from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class MlflowPathsModel(BasePathsModel):
    experiments: Optional[str]
    runs: Optional[str]
    models: Optional[str]
    model_versions: Optional[str]

    class Config:
        dependencies_map = {
            "experiments": ("experiments",),
            "runs": ("experiments", "runs"),
            "models": ("models",),
            "model_versions": ("models", "model_versions"),
        }


class MlFlowGenerator(Generator):
    source = "mlflow"
    server_model = HostnameModel
    paths_model = MlflowPathsModel
