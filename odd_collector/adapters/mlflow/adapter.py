from typing import Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import CubeJSPlugin
from odd_collector.domain.predefined_data_source import create_predefined_datasource

from .generator import MlFlowGenerator
from .logger import logger
from .mappers.experiment import map_experiment
from .mappers.job import map_job
from .client import MlflowHelper, MlflowClientBase


class Adapter(AbstractAdapter):
    def __init__(
            self,
            config: CubeJSPlugin,
            client: Type[MlflowClientBase] = MlflowHelper
    ):
        self.config = config
        self.repo = client(config)
        self.generator = MlFlowGenerator()

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self):
        logger.debug("Start collecting")
        pipeline_info = await self.repo.get_experiment_info()
        job_info = await self.repo.get_job_info()

        logger.debug("End collecting")
        pipeline_entities = [map_experiment(self.generator, pipeline) for pipeline in pipeline_info]
        job_entities = [map_job(self.generator, job) for job in job_info]

        return DataEntityList(data_source_oddrn=self.get_data_source_oddrn(), items=pipeline_entities), \
               DataEntityList(items=job_entities)
