from datetime import datetime

from mlflow.entities.run import Run

from odd_collector.adapters.mlflow.generator import MlFlowGenerator


class Job:
    def __init__(
        self,
        name: str,
        experiment_id: str,
        run_id: str,
        status: str,
        artifact_uri: str,
        start_time: datetime,
        end_time: datetime,
        metrics: dict,
        job_params: dict,
        tags: str,
        artifacts: list,
        input_artifacts: list,
        output_artifacts: list
    ):
        self.name = name
        self.experiment_id = experiment_id
        self.run_id = run_id
        self.status = status
        self.artifact_uri = artifact_uri
        self.start_time = start_time
        self.end_time = end_time
        self.metrics = metrics
        self.job_params = job_params
        self.tags = tags
        self.artifacts = artifacts
        self.input_artifacts = input_artifacts
        self.output_artifacts = output_artifacts

    @staticmethod
    def from_response(job: Run, artifacts: list):
        input_artifacts = job.data.params.get('input_artifacts', [])
        output_artifacts = job.data.params.get('output_artifacts', [])

        return Job(
            name=job.info.run_name,
            experiment_id=job.info.experiment_id,
            run_id=job.info.run_id,
            status=job.info.status,
            artifact_uri=job.info.artifact_uri,
            start_time=job.info.start_time,
            end_time=job.info.end_time,
            metrics=job.data.metrics,
            job_params=job.data.params,
            tags=job.data.tags,
            artifacts=artifacts,
            input_artifacts=input_artifacts,
            output_artifacts=output_artifacts
        )

    def get_oddrn(self, oddrn_generator: MlFlowGenerator):
        oddrn_generator.get_oddrn_by_path("jobs", self.run_id)
        return oddrn_generator.get_oddrn_by_path("jobs")

