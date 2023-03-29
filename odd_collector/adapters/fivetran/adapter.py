from odd_models.models import DataEntityList
from oddrn_generator import FivetranGenerator

from .helpers import DatasetGenerator
from .mappers.transformers import map_transformers
from .repository import FivetranRepository
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector.domain.plugin import FivetranPlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: FivetranPlugin):
        self._repo = FivetranRepository(config)
        self._generator = FivetranGenerator(host_settings=config.base_url)

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        connector_details = self._repo.get_connector_details()
        destination_details = self._repo.get_destination_details()
        connector = DatasetGenerator.get_generator(
            connector_details.service, **connector_details.config
        )
        destination = DatasetGenerator.get_generator(
            destination_details.service, **destination_details.config
        )
        entities = [
            map_transformers(
                self._generator,
                [connector.get_data_source_oddrn()],
                [destination.get_data_source_oddrn()],
            )
        ]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=entities,
        )
