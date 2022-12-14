from typing import Type, List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList, DataEntity

from odd_collector.domain.plugin import MlflowPlugin

from .generator import MlFlowGenerator
from .logger import logger
from .mappers.experiment import map_experiment
from .mappers.job import map_job
from .client import MlflowHelper, MlflowClientBase


class Adapter(AbstractAdapter):
    def __init__(
            self,
            config: MlflowPlugin,
            client: Type[MlflowClientBase] = MlflowHelper
    ):
        self.config = config
        self.repo = client(config)
        self.generator = MlFlowGenerator(host_settings=config.host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        logger.debug("Start collecting")
        experiments = self.repo.get_experiment_info()
        logger.debug("End collecting")

        experiments_entities: List[DataEntity] = []
        jobs_entities: List[DataEntity] = []

        for single_experiment in experiments:
            self.generator.set_oddrn_paths(experiments=single_experiment.name)

            jobs = single_experiment.jobs
            job_entities = [map_job(self.generator, single_job) for single_job in jobs]

            jobs_entities.extend(job_entities)
            experiment_entity = map_experiment(self.generator, single_experiment, [job.oddrn for job in job_entities])
            experiments_entities.append(experiment_entity)

        return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[*experiments_entities, *jobs_entities])
