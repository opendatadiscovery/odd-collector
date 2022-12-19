from typing import Any, Dict, List

from mlflow.entities import Experiment as MLFlowExperiment

from .run import Run
from datetime import datetime
from typing import Optional
from odd_collector.helpers.datetime_from_ms import datetime_from_milliseconds

class Experiment:
    def __init__(
        self,
        name: str,
        experiment_id: str,
        tags: Dict[str, Any],
        lifecycle_stage: str,
        creation_time: datetime,
        last_update_time: Optional[datetime],
        artifact_location: str,
        runs: List[Run],
    ):
        self.name = name
        self.experiment_id = experiment_id
        self.tags = tags
        self.lifecycle_stage = lifecycle_stage
        self.creation_time = creation_time
        self.last_update_time = last_update_time
        self.artifact_location = artifact_location
        self.runs: List[Run] = runs

    @classmethod
    def from_mlflow(cls, experiment: MLFlowExperiment, runs: List[Run]):
        return cls(
            name=experiment.name,
            experiment_id=experiment.experiment_id,
            tags=experiment.tags,
            lifecycle_stage=experiment.lifecycle_stage,
            creation_time=datetime_from_milliseconds(experiment.creation_time),
            last_update_time=datetime_from_milliseconds(experiment.last_update_time),
            artifact_location=experiment.artifact_location,
            runs=runs,
        )

    @property
    def metadata(self) -> Dict[str, any]:
        return {
            "experiment_id": self.experiment_id,
            **self.tags,
            "lifecycle_stage": self.lifecycle_stage,
            "artifact_location": self.artifact_location,
        }
