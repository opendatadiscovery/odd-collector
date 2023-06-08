import logging
from typing import Iterable, Dict, Optional

from funcy import get_lax
from elasticsearch import Elasticsearch
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ElasticSearchGenerator

from .mappers.stream import map_data_stream, map_data_stream_template
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

        logger.debug(f"Indeces are {indices}")

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

        logger.debug("Process data streams and their templates")
        all_data_streams = self.__get_data_streams()

        logger.debug("Build template to data stream mapping")
        templates_info = self.get_templates_from_data_streams(all_data_streams)

        for template, data_streams in templates_info.items():
            template_meta = self.__get_data_stream_templates_info(template).get(
                "index_templates"
            )

            data_stream_entities = []
            logger.debug(f"Template {template} has metadata {template_meta}")

            for data_stream in data_streams:
                logger.debug(
                    f"Data stream {data_stream['name']} has template {template}"
                )

                lifecycle_policies = self.__get_rollover_policy(data_stream)
                stream_data_entity = map_data_stream(
                    data_stream,
                    template_meta,
                    lifecycle_policies,
                    self.__oddrn_generator,
                )

                data_stream_entities.append(stream_data_entity)

            result.extend(data_stream_entities)

            logger.debug(f"Create template data entity {template}")

            data_streams_oddrn = [item.oddrn for item in data_stream_entities]
            logger.debug(f"List of data streams oddrn {data_streams_oddrn}")

            template_entity = map_data_stream_template(
                template_meta, data_streams_oddrn, self.__oddrn_generator
            )
            result.append(template_entity)

        return result

    def __get_rollover_policy(self, stream_data: Dict) -> Optional[Dict]:
        try:
            backing_indices = [
                index_info["index_name"] for index_info in stream_data["indices"]
            ]
            for index in backing_indices:

                index_settings = self.__es_client.indices.get(index=index)
                lifecycle_policy = get_lax(
                    index_settings, [index, "settings", "index", "lifecycle"]
                )

                if lifecycle_policy:
                    logger.debug(
                        f"Index {index} has Lifecycle Policy {lifecycle_policy['name']}"
                    )
                    lifecycle_policy_data = self.__es_client.ilm.get_lifecycle(
                        policy=lifecycle_policy["name"]
                    )

                    logger.debug(f"Lifecycle policy metadata {lifecycle_policy_data}")

                    rollover = get_lax(
                        lifecycle_policy_data,
                        [
                            lifecycle_policy["name"],
                            "policy",
                            "phases",
                            "hot",
                            "actions",
                            "rollover",
                        ],
                    )

                    if rollover is not None:
                        max_size = rollover.get("max_size")
                        max_age = rollover.get("max_age")
                    else:
                        max_size = None
                        max_age = None

                    lifecycle_metadata = {"max_age": max_age, "max_size": max_size}
                    return lifecycle_metadata

                else:
                    logger.debug(f"No lifecycle policy exists for this index {index}.")
                    return None
        except KeyError:
            logger.debug(f"Incorrect fields. Got fields: {stream_data}")
            return None

    def get_templates_from_data_streams(self, data_streams: Dict) -> Dict:
        """
        Expected result
        {
             "template": [data_stream, data_stream1],
             "another_template": [data_stream2]
        }
        """
        templates = {}
        for data_stream in data_streams:
            if data_stream["template"] not in templates:
                templates[data_stream["template"]] = [data_stream]
            else:
                templates[data_stream["template"]].append(data_stream)
        return templates

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
        return response["data_streams"]

    def __get_data_stream_templates_info(self, template_name: str) -> Dict:
        response = self.__es_client.indices.get_index_template(name=template_name)
        return response

    def __process_index_data(self, index_name: str, index_mapping: dict):
        mapping = index_mapping["mappings"]["properties"]
        logger.debug(f"Process mapping for index {index_name} with mapping {mapping}")
        return map_index(index_name, mapping, self.__oddrn_generator)
