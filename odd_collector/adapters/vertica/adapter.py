import logging
from typing import List

from odd_models.models import DataEntity, DataEntityList
from odd_collector_sdk.domain.adapter import AbstractAdapter
from .vertica_generator import VerticaGenerator


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__oddrn_generator = VerticaGenerator(
            host_settings=config.host,
            databases=config.database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        pass