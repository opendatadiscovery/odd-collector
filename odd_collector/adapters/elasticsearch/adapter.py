import logging
from typing import Iterable

from elasticsearch import Elasticsearch
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ElasticSearchGenerator

from .mappers.indexes import map_index


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__es_client = Elasticsearch(
            config.host,
            port=config.port,
            http_auth=config.http_auth,
            use_ssl=config.use_ssl,
            verify_certs=config.verify_certs,
            ca_certs=config.ca_certs,
        )
        self.__oddrn_generator = ElasticSearchGenerator(host_settings=config.host)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_datasets(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_datasets(self) -> Iterable[DataEntity]:
        result = []
        indices = self.__get_indices()
        for index in indices:
            mapping = self.__get_mapping(index["index"])[index["index"]]
            try:
                result.append(self.__process_index_data(index, mapping))
            except KeyError as e:
                logging.warning(
                    f"Elasticsearch adapter failed to process index {index}: KeyError {e}"
                )
        return result

    def __get_mapping(self, index_name: str):
        return self.__es_client.indices.get_mapping(index_name)

    def __get_indices(self):
        # System indices startswith `.` character
        return [
            _
            for _ in self.__es_client.cat.indices(format="json")
            if not _["index"].startswith(".")
        ]

    def __process_index_data(self, index_name: str, index_mapping: dict):
        mapping = index_mapping["mappings"]["properties"]
        return map_index(index_name, mapping, self.__oddrn_generator)
