from datetime import datetime

from mlflow.entities.run import Run

from odd_collector.adapters.mlflow.generator import MlFlowGenerator


class Job:
    def __init__(
        self,
        experiment_id: str,
        run_id: str,
        status: str,
        artifact_uri: str,
        start_time: datetime,
        end_time: datetime,
        metrics: dict,
        job_params: dict,
        user_id: str
    ):
        self.experiment_id = experiment_id
        self.run_id = run_id
        self.status = status
        self.artifact_uri = artifact_uri
        self.start_time = start_time
        self.end_time = end_time
        self.metrics = metrics
        self.job_params = job_params
        self.user_id = user_id

    @staticmethod
    def from_response(job: Run):
        return Job(
            experiment_id=job.info.experiment_id,
            run_id=job.info.run_id,
            status=job.info.status,
            artifact_uri=job.info.artifact_uri,
            start_time=job.info.start_time,
            end_time=job.info.end_time,
            metrics=job.data.metrics,
            job_params=job.data.params,
            user_id=job.data.tags
        )

    def get_oddrn(self, oddrn_generator: MlFlowGenerator):
        oddrn_generator.get_oddrn_by_path("job", self.run_id)
        return oddrn_generator.get_oddrn_by_path("job")

