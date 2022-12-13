from abc import ABC
from typing import List, Optional, Callable

from mlflow.entities import Experiment
from odd_collector_sdk.errors import DataSourceAuthorizationError

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from ...domain.plugin import MlflowPlugin
from .domain.experiment import ExperimentEntity
from .domain.job import Job
from funcy import lfilter


class MlflowClientBase(ABC):
    def __init__(self, config: MlflowPlugin):
        self.config = config
        self._dev_mode = config.dev_mode
        self._token = config.token
        self._host = config.host
        self._experiments = config.experiments

    def get_experiment_info(self) -> List[ExperimentEntity]:
        raise NotImplementedError

    def get_job_info(self, experiment_entity) -> List[Job]:
        raise NotImplementedError


class MlflowHelper(MlflowClientBase):
    API_PATH = "mlflow-api/v1"

    def __init__(self, config: MlflowPlugin):
        super().__init__(config)
        self._client = MlflowClient()
        try:
            mlflow.set_tracking_uri(self._host)
        except Exception as e:
            raise MlflowException(f"Error while creating mlflow client: {self._host}, {e}")

    def get_experiment_info(self) -> List[Experiment]:
        try:
            experiment_entity_list = self._filter_experiments_by_name()

            return [
                ExperimentEntity.from_response(experiment, self.get_job_info(experiment))
                for experiment in experiment_entity_list
            ]
        except Exception as e:
            raise DataSourceAuthorizationError(
                f"Couldn't connect to mlflow: {self._host}"
            ) from e

    def _filter_experiments_by_name(self) -> List[Experiment]:
        experiments = mlflow.search_experiments(order_by=["name"])
        return lfilter(self._experiment_name_matches(), experiments)

    def _experiment_name_matches(self) -> Optional[Callable[[str], bool]]:
        if self._experiments:
            return lambda name: name in self._experiments
        else:
            return None

    def _get_experiment_job_id(self, experiment_entity: Experiment) -> list:
        """
            Request experiment's jobs as a dataframe
            iterate to generate a list of jobs (runs) that belongs to specified Experiment
        Returns:
            object: get list of job_ids for specific experiment
        """
        jobs_df = mlflow.search_runs(experiment_entity.experiment_id)
        return [
            job.run_id
            for index, job in jobs_df.iterrows()
        ]

    def _get_job_artifacts(self, run_id):
        """
            Collect list of all artifacts. As we can't fetch separately input/ output artifacts.
            For each artifact folder -> go inside and insert to a general list
        Args:
            run_id:
        Returns:
            list of artifacts for specified run_id
        """
        available_artifacts = [f.path for f in self._client.list_artifacts(run_id)]
        all_job_id_artifacts = []
        for artifact in available_artifacts:
            all_job_id_artifacts.append([f.path for f in self._client.list_artifacts(run_id, artifact)]) \
                if self.is_folder(artifact) else all_job_id_artifacts.append(artifact)
        return all_job_id_artifacts

    def is_folder(self, artifact: str) -> bool:
        return '.' not in artifact

    def get_job_info(self, experiment_entity) -> List[Job]:
        try:
            experiment_job_list = []
            for job_id in self._get_experiment_job_id(experiment_entity):
                job_info = self._client.get_run(job_id)
                experiment_job_list.append(Job.from_response(job_info, self._get_job_artifacts(job_id)))
            return experiment_job_list
        except Exception as e:
            raise DataSourceAuthorizationError(
                f"Couldn't connect to mlflow: {self._host}"
            ) from e
