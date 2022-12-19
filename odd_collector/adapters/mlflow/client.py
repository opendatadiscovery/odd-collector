import logging
import tempfile
from abc import ABC
from functools import partial
from typing import Callable, Iterable, Optional, TypeVar

import mlflow
from mlflow.entities import Experiment as MlFlowExperiment
from mlflow.entities import FileInfo
from mlflow.entities import Run as MlFLowRun
from mlflow.entities.model_registry import ModelVersion as MlFlowModelVersion
from mlflow.entities.model_registry import RegisteredModel
from mlflow.exceptions import MlflowException
from mlflow.store.entities import PagedList
from odd_collector_sdk.errors import DataSourceError

from .domain.odd_metadata import OddMetadata
from ...domain.plugin import MlflowPlugin
from .domain.experiment import Experiment
from .domain.model import Model
from .domain.model_version import ModelVersion
from .domain.run import Run

T = TypeVar("T", MlFlowExperiment, MlFLowRun, RegisteredModel, MlFlowModelVersion)


class MlflowClientBase(ABC):
    def __init__(self, config: MlflowPlugin):
        self.config = config

    def get_experiments(self) -> Iterable[Experiment]:
        raise NotImplementedError

    def get_models(self) -> Iterable[Model]:
        raise NotImplementedError


class Client(MlflowClientBase):
    METADATA_FILE_NAME = 'odd_metadata.json'

    def __init__(self, config: MlflowPlugin):
        super().__init__(config)
        try:
            self._client = mlflow.MlflowClient(config.tracking_uri, config.registry_uri)
            mlflow.set_tracking_uri(config.tracking_uri)
            mlflow.set_registry_uri(config.registry_uri)
        except MlflowException as e:
            raise DataSourceError from e

    def get_experiments(self) -> Iterable[Experiment]:
        """Load and map mlflow Experiments with runs

        Returns:
            Iterable[Experiment]: mapped to domain Experiments

        """
        search_experiments = partial(
            self._client.search_experiments, filter_string=self._filter_string()
        )

        for experiment in self._fetch(fn=search_experiments):
            runs = list(self._get_runs(experiment))
            yield Experiment.from_mlflow(experiment, runs)

    def _filter_string(self) -> Optional[str]:
        """Generate filter string by experiments name

        Returns:
            Optional[str]
        """
        if self.config.filter_experiments is None:
            return None

        return " AND ".join(
            f"name == '{name}'" for name in self.config.filter_experiments
        )

    def get_models(self) -> Iterable[Model]:
        """Search registered model and all model versions for them (not only latest)

        Returns:
            Iterable[Model]: mapped to domain Models
        """
        search_models = partial(self._client.search_registered_models)

        for model in self._fetch(fn=search_models):
            model = Model.from_mlflow(model)
            model.model_versions = list(self._get_model_versions_by(model.name))

            yield model

    def _get_model_versions_by(self, model_name: str) -> Iterable[ModelVersion]:
        """Search all model versions by model name

        Args:
            model_name (str): MLFlow Model's name

        Returns:
            Iterable[ModelVersion]: _description_
        """
        search_model_versions = partial(
            self._client.search_model_versions, filter_string=f"name='{model_name}'"
        )

        for mv in self._fetch(fn=search_model_versions):
            yield ModelVersion.from_mlflow(mv)

    def _get_runs(self, experiment: MlFlowExperiment) -> Iterable[Run]:
        search_runs = partial(
            self._client.search_runs, experiment_ids=[experiment.experiment_id]
        )

        for run in self._fetch(fn=search_runs):
            artifacts = self._get_artifacts(run.info.run_id)
            odd_artifacts = self._load_odd_artifact(run.info.run_id)

            yield Run.from_mlflow(run, list(artifacts), odd_artifacts)

    def _get_artifacts(self, run_id: str) -> Iterable[FileInfo]:
        """
            Collect list of all artifacts. As we can't fetch separately input/ output artifacts.
            For each artifact folder -> go inside and insert to a general list
        Args:
            run_id: str
        Returns:
            list of artifacts for specified run_id
        """

        def _recursive(file_info: FileInfo) -> Iterable[FileInfo]:
            if file_info.is_dir:
                for file_info in self._client.list_artifacts(
                        run_id, path=file_info.path
                ):
                    yield from _recursive(file_info)
            else:
                yield file_info

        for file_info in self._client.list_artifacts(run_id):
            yield from _recursive(file_info)

    def _fetch(self, fn: Callable[[Optional[str]], PagedList[T]]) -> Iterable[T]:
        """Helper function fetching paginated resources recursively using token page

        Args:
            fn (function): MLFlow partial applied function

        Returns:
            Iterable[Any]: _description_
        """
        first_page: PagedList = fn()
        yield from first_page

        token = first_page.token
        while token:
            next_page = fn(page_token=token)

            yield from next_page

            token = next_page.token

    def _load_odd_artifact(self, run_id: str) -> OddMetadata:
        """
        When MlFlow user logged additional information for Run as:
        mlflow.log_dict(
            {"inputs": ["s3://training/wine/winequality-red.csv"], "outputs": []},
            "odd_metadata.json",
        )

        Adapter tries to find artifact with name odd_metadata.json,
        load it to temporary directory and parse to OddMetadata

        """
        try:
            with tempfile.TemporaryDirectory() as tmp_dir:
                file_path = mlflow.artifacts.download_artifacts(
                    run_id=run_id,
                    artifact_path=self.METADATA_FILE_NAME,
                    dst_path=tmp_dir
                )

                return OddMetadata.parse_file(file_path)
        except Exception as e:
            logging.debug("Could not read metadata file odd_metadata.json", e, exc_info=True)
            return OddMetadata()
