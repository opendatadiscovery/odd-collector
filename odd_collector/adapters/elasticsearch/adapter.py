import logging
from typing import Iterable, Dict

from elasticsearch import Elasticsearch
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ElasticSearchGenerator

from .mappers.stream import map_stream
from .mappers.indexes import map_index
from .logger import logger


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
        logger.debug("Collect dataset")
        result = []
        indices = self.__get_indices()
        data_streams_info = self.__get_data_streams()

        logger.debug(f"Indeces are {indices}")
        logger.debug(f"Data streams are {data_streams_info}")

        logger.debug("Process indeces")
        for index in indices:
            mapping = self.__get_mapping(index["index"])[index["index"]]
            logger.debug(f"Mapping for index {index['index']} is {mapping}")
            try:
                result.append(self.__process_index_data(index, mapping))
            except KeyError as e:
                logging.warning(
                    f"Elasticsearch adapter failed to process index {index}: KeyError {e}"
                )

        logger.debug("Process data streams")
        for item in data_streams_info['data_streams']:
            data_entity = self.__process_stream_data(item)
            result.append(data_entity)
        return result

    def __get_mapping(self, index_name: str):
        return self.__es_client.indices.get_mapping(index_name)

    def __get_indices(self):
        # System indices startswith `.` character
        logger.debug("Get system indeces start with .")
        return [
            _
            for _ in self.__es_client.cat.indices(format="json")
            if not _["index"].startswith(".")
        ]

    def __get_data_streams(self) -> Dict:
        response = self.__es_client.indices.get_data_stream("*")
        return response

    def __process_stream_data(self, data_stream):
        return map_stream(data_stream, self.__oddrn_generator)

    def __process_index_data(self, index_name: str, index_mapping: dict):
        mapping = index_mapping["mappings"]["properties"]
        logger.debug(f"Process mapping for index {index_name} with mapping {mapping}")
        return map_index(index_name, mapping, self.__oddrn_generator)
