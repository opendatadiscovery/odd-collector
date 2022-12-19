from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from funcy import lpluck_attr
from mlflow.entities.run import Run as MLfLowRun
from mlflow.entities.run import RunData, RunInfo

from odd_collector.adapters.mlflow.generator import MlFlowGenerator
from odd_collector.helpers.datetime_from_ms import datetime_from_milliseconds
from odd_collector.helpers.flatdict import FlatDict

from .odd_metadata import OddMetadata


@dataclass
class Run:
    name: str
    experiment_id: str
    run_id: str
    status: str
    artifact_uri: str
    start_time: datetime
    end_time: datetime
    metrics: dict
    job_params: dict
    tags: Dict[str, Any]
    artifacts: List
    odd_metadata: OddMetadata
    user: Optional[str]

    @classmethod
    def from_mlflow(cls, run: MLfLowRun, artifacts: List, odd_artifact: OddMetadata):
        data: RunData = run.data
        info: RunInfo = run.info

        return cls(
            name=info.run_name,
            experiment_id=info.experiment_id,
            run_id=info.run_id,
            status=info.status,
            artifact_uri=info.artifact_uri,
            start_time=datetime_from_milliseconds(info.start_time),
            end_time=datetime_from_milliseconds(info.end_time),
            metrics=data.metrics,
            job_params=data.params,
            tags=data.tags,
            artifacts=artifacts,
            odd_metadata=odd_artifact,
            user=data.tags.get("mlflow.user"),
        )

    def get_oddrn(self, oddrn_generator: MlFlowGenerator):
        oddrn_generator.get_oddrn_by_path("jobs", self.run_id)
        return oddrn_generator.get_oddrn_by_path("jobs")

    @property
    def metadata(self) -> Dict[str, Any]:
        breakpoint()
        return {
            "run_id": self.run_id,
            "status": self.status,
            "artifact_uri": self.artifact_uri,
            "artifacts": ", ".join(lpluck_attr("path", self.artifacts)),
            **FlatDict(self.job_params),
            **FlatDict(self.metrics),
            **FlatDict(self.tags),
        }
