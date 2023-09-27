from typing import Optional
from urllib.parse import urlparse

from funcy import get_in, get_lax
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ElasticSearchGenerator, Generator

from odd_collector.domain.plugin import ElasticsearchPlugin

from .client import Client
from .logger import logger
from .mappers.indices import map_index
from .mappers.stream import map_data_stream
from .mappers.template import TemplateEntity, map_template


class Adapter(BaseAdapter):
    config: ElasticsearchPlugin
    generator: ElasticSearchGenerator

    def __init__(self, config: ElasticsearchPlugin) -> None:
        super().__init__(config)
        self.client = Client(config)

    def create_generator(self) -> Generator:
        return ElasticSearchGenerator(host_settings=urlparse(self.config.host).netloc)

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=list(self.get_datasets()),
        )

    def get_datasets(self) -> list[DataEntity]:
        logger.debug(
            f"Start collecting datasets from Elasticsearch at {self.config.host} with port {self.config.port}"
        )

        indices = self.client.get_indices("*")
        templates = self.client.get_index_template("*")

        mappings = self.client.get_mapping()
        data_streams = self.client.get_data_streams()

        indices = [
            index for index in indices if not index["index"].startswith(".internal")
        ]
        logger.success(f"Got {len(indices)} indices")

        index_by_names = {index["index"]: index for index in indices}
        templates_by_names = {
            tmpl["name"]: tmpl for tmpl in templates if not tmpl["name"].startswith(".")
        }
        streams_by_names = {stream["name"]: stream for stream in data_streams}
        mappings_by_names = dict(mappings.items())

        indices_entities: dict[str, DataEntity] = {}
        for index_name, index in index_by_names.items():
            indices_entities[index_name] = map_index(
                index=index,
                generator=self.generator,
                properties=get_in(
                    mappings_by_names,
                    [index_name, "mappings", "properties"],
                    default={},
                ),
            )

        # map templates
        template_entities: dict[str, TemplateEntity] = {}
        for tmpl_name, tmpl in templates_by_names.items():
            data_entity = map_template(tmpl, self.generator)
            pattern = tmpl["index_template"]["index_patterns"]

            # Here we are trying to get all indices that match the pattern
            # to show that current template works with index
            # But if we can't get them, we just skip
            try:
                for index_name in self.client.get_indices(index=pattern, h="index"):
                    if index_entity := indices_entities.get(index_name["index"]):
                        data_entity.add_output(index_entity)
            except Exception as e:
                logger.warning(e)
                continue

            template_entities[tmpl_name] = data_entity

        # map data streams
        stream_entities = {}
        for stream_name, stream in streams_by_names.items():
            stream_data_entity = map_data_stream(stream, self.generator)
            stream_entities[stream_name] = stream_data_entity

            if template_entity := template_entities.get(stream["template"]):
                template_entity.add_input(stream_data_entity)

        return [
            *indices_entities.values(),
            *stream_entities.values(),
            *template_entities.values(),
        ]

    # TODO: implement mapping rollover policies
    def _get_rollover_policy(self, stream_data: dict) -> Optional[dict]:
        try:
            backing_indices = [
                index_info["index_name"] for index_info in stream_data["indices"]
            ]
            for index in backing_indices:
                index_settings = self.client.get_indices(index)
                lifecycle_policy = get_lax(
                    index_settings, [index, "settings", "index", "lifecycle"]
                )

                if lifecycle_policy:
                    logger.debug(
                        f"Index {index} has Lifecycle Policy {lifecycle_policy['name']}"
                    )
                    lifecycle_policy_data = self.client.ilm.get_lifecycle(
                        name=lifecycle_policy["name"]
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
