import json
import logging
import urllib.request

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import DbtGenerator

from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__url = config.odd_catalog_url
        self.__oddrn_generator = DbtGenerator(host_settings=config.host)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_datasets(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_datasets(self) -> list[DataEntity]:
        try:

            with urllib.request.urlopen(f"{self.__url}catalog.json") as catalog_file:
                catalog_json: dict = json.load(catalog_file)

            with urllib.request.urlopen(f"{self.__url}manifest.json") as manifest_file:
                manifest_json: dict = json.load(manifest_file)

            tables: dict = catalog_json["nodes"]

            return map_table(self.__oddrn_generator, tables, manifest_json)

        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        return []
