from abc import ABC
from typing import List

from odd_collector_sdk.errors import DataSourceAuthorizationError

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from ...domain.plugin import MlflowPlugin
from .domain.experiment import ExperimentEntity
from .domain.job import Job


class MlflowClientBase(ABC):
    def __init__(self, config: MlflowPlugin):
        self.config = config
        self._dev_mode = config.dev_mode
        self._token = config.token
        self._tracking_url = config.tracking_url

    async def get_experiment_info(self) -> List[ExperimentEntity]:
        raise NotImplementedError

    async def get_job_info(self) -> List[Job]:
        raise NotImplementedError


class MlflowHelper(MlflowClientBase):
    API_PATH = "mlflow-api/v1"

    def __init__(self, config: MlflowPlugin):
        super().__init__(config)
        try:
            mlflow.set_tracking_uri(self._tracking_url)
        except Exception as e:
            raise MlflowException(f"Error while creating mlflow client: {self._tracking_url}, {e}")

    def get_experiment_info(self) -> List[ExperimentEntity]:
        try:
            client = MlflowClient()
            experiment_details = client.get_experiment_by_name('test_experiment')
            return ExperimentEntity.from_response(experiment_details)
        except Exception as e:
            raise DataSourceAuthorizationError(
                f"Couldn't connect to mlflow: {self._tracking_url}"
            ) from e

    def get_job_info(self) -> List[Job]:
        try:
            client = MlflowClient()
            for experiment in mlflow.search_experiments(order_by=["name"]):
                job_df = mlflow.search_runs(experiment.experiment_id)
                for index, job in job_df.iterrows():
                    job_info = client.get_run(job.run_id)
                    return Job.from_response(job_info)
        except Exception as e:
            raise DataSourceAuthorizationError(
                f"Couldn't connect to mlflow: {self._tracking_url}"
            ) from e
