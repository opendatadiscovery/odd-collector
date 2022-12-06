from datetime import datetime

from ..generator import MlFlowGenerator

from mlflow.entities.experiment import Experiment


class ExperimentEntity:
    def __init__(
        self,
        name: str,
        experiment_id: str,
        tags: str,
        lifecycle_stage: str,
        creation_time: datetime,
        last_update_time: datetime,
        artifact_location: str,
    ):
        self.name = name
        self.experiment_id = experiment_id
        self.tags = tags
        self.lifecycle_stage = lifecycle_stage
        self.creation_time = creation_time
        self.last_update_time = last_update_time,
        self.artifact_location = artifact_location,


    @staticmethod
    def from_response(response: Experiment):
        return ExperimentEntity(
            name=response.name,
            experiment_id=response.experiment_id,
            tags=response.tags,
            lifecycle_stage=response.lifecycle_stage,
            creation_time=response.creation_time,
            last_update_time=response.last_update_time,
            artifact_location=response.artifact_location,
        )

    def get_oddrn(self, oddrn_generator: MlFlowGenerator):
        oddrn_generator.get_oddrn_by_path("experiment", self.name)
        return oddrn_generator.get_oddrn_by_path("experiment")
