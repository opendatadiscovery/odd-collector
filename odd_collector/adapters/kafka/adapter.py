import json
import logging
from typing import Dict, List

import requests
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList

from .kafka_generator import KafkaGenerator
from .mappers.schemas import map_topics

FORMAT = "%(asctime)s | %(levelname)s | %(name)s  | %(message)s"

logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger("KafkaAdapter")
logger.setLevel(logging.INFO)


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.schema_registry_conf = config.schema_registry_conf
        self.broker_conf = config.broker_conf
        logging.debug(config.broker_conf)
        logging.debug(config.schema_registry_conf)
        if not self.broker_conf.get("group.id"):
            self.broker_conf["group.id"] = "odd_collector"
            logger.warning("setting group.id to odd_collector")
        if not self.broker_conf.get("bootstrap.servers"):
            logger.error("'bootstrap.servers' is not specified in broker_conf")
            raise "'bootstrap.servers' is not specified in broker_conf"
        self.__oddrn_generator = KafkaGenerator(
            host_settings=self.broker_conf.get("bootstrap.servers"),
            clusters=self.broker_conf.get("bootstrap.servers").replace("/", ":"),
        )
        self.admin_client = AdminClient(self.broker_conf)
        try:
            self.schema_client = (
                SchemaRegistryClient(self.schema_registry_conf)
                if self.schema_registry_conf.get("url")
                else None
            )
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Unable to connect to schema registry on url '{self.schema_registry_conf.get('url')}', schema will be skipped"
            )
            self.schema_client = None
            self.schema_subjects = None
        self.consumer = Consumer(self.broker_conf)

    def get_data_source_oddrn(self) -> str:
        logging.debug(self.__oddrn_generator.get_data_source_oddrn())
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        This function will convert a dictionary schema into
        an odd list of data entities
        """
        try:
            self.schema_subjects = (
                self.schema_client.get_subjects() if self.schema_client else None
            )

            topics = self.retrieve_schemas()
            # logging.debug("*************SCHEMA******************")
            logging.debug(topics)
            # logging.debug("*************SCHEMA******************")

            return map_topics(
                self.__oddrn_generator,
                topics,
                self.broker_conf.get("bootstrap.servers"),
                self.schema_client,
            )
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)

        return []

    def get_data_entity_list(self) -> DataEntityList:
        res = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
        logging.debug("*************DataEntity******************")
        logging.debug(res.json())
        logging.debug("*************DataEntity******************")
        return res

    def retrieve_schemas(self):

        try:
            topics = self.admin_client.list_topics()

            schemas = []
            for topic in topics.topics.items():
                schema = {
                    "title": topic[
                        0
                    ]  # TODO add row number, creation and modification date  like in mongo if possible
                }
                metadata = {}
                metadata["partitions"] = len(topic[1].partitions.keys())
                schema["metadata"] = metadata
                msg = None

                if self.schema_subjects and f"{topic[0]}-value" in self.schema_subjects:
                    schema["value"] = eval(
                        self.schema_client.get_latest_version(
                            f"{topic[0]}-value"
                        ).schema.schema_str
                    )
                    logger.info(
                        f"Found schema in schema registry for topic: {topic[0]}"
                    )
                else:
                    schema["value"] = {"type": "non-registry"}
                    schema["value"]["data"] = None

                if self.schema_subjects and f"{topic[0]}-key" in self.schema_subjects:
                    schema["key"] = eval(
                        self.schema_client.get_latest_version(
                            f"{topic[0]}-key"
                        ).schema.schema_str
                    )
                    logger.info(
                        f"Found  key schema in schema registry for topic: {topic[0]}"
                    )
                else:
                    schema["key"] = {"type": "non-registry"}
                    schema["key"]["data"] = None

                schemas.append(schema)
            return schemas

        except Exception as e:
            logging.debug("something wrong with the schemas!")
