import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, KafkaException, TopicPartition
from odd_models.models import DataEntity, DataEntityList
from typing import List, Dict
from .mappers.schemas import map_topics
from oddrn_generator import KafkaGenerator
from odd_collector_sdk.domain.adapter import AbstractAdapter
import json

FORMAT = '%(asctime)s | %(levelname)s | %(name)s  | %(message)s'

logging.basicConfig(level=logging.INFO, format=FORMAT)
logger = logging.getLogger('KafkaAdapter')
logger.setLevel(logging.INFO)


class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        self.schema_registry_conf = config.schema_registry_conf
        self.broker_conf = config.broker_conf
        if not self.broker_conf.get("group.id"):
            self.broker_conf["group.id"] = "odd_collector" 
            logger.warning("setting group.id to odd_collector")
        if not self.broker_conf.get('bootstrap.servers'):
            logger.error("'bootstrap.servers' is not specified in broker_conf")
            raise "'bootstrap.servers' is not specified in broker_conf"
        self.__oddrn_generator = KafkaGenerator(host_settings=self.broker_conf.get('bootstrap.servers'))
        self.admin_client = AdminClient(self.broker_conf)
        self.schema_client = SchemaRegistryClient(self.schema_registry_conf) if self.schema_registry_conf.get('url') else None
        self.schema_subjects = self.schema_client.get_subjects() if self.schema_client else None
        self.consumer = Consumer(self.broker_conf)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        This function will convert a dictionary schema into
        an odd list of data entities
        """
        try:
            schemas = self.retrive_scheams()
            print("*************SCHEMA******************")
            print(schemas)
            print("*************SCHEMA******************")

            return map_topics(self.__oddrn_generator, schemas, self.broker_conf.get('bootstrap.servers'))
        except Exception as e:
            logging.error('Failed to load metadata for tables')
            logging.exception(e)

        return []
    
    def get_data_entity_list(self) -> DataEntityList:
        res = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
        print("*************DataEntity******************")
        print(res.json())
        print("*************DataEntity******************")
        return res

    def retrive_scheams(self) :

        try:
            topics = self.admin_client.list_topics()

            schemas = []
            for topic in topics.topics.items():
                schema = {"title": topic[0]  #TODO add row number, creation and modification date  like in mongo if possible 
                        }
                metadata = {}
                metadata['partitions'] = len(topic[1].partitions.keys())
                schema["metadata"]=metadata

                if self.schema_subjects and f"{topic[0]}-value" in self.schema_subjects:
                    schema['value'] = eval(self.schema_client.get_latest_version(f"{topic[0]}-value").schema.schema_str)
                    logger.info(f"Found schema in schema registry for topic: {topic[0]}")
                else:
                    logger.info(f"Did not find schema in schema registry for topic: {topic[0]}, trying to pars as json")
                    partition = TopicPartition(topic[0],max(topic[1].partitions.keys()) )
                    offset = self.consumer.offsets_for_times([partition])
                    self.consumer.assign(offset)
                    try:
                        msg = self.consumer.consume(1, timeout=3)[0]
                        if msg is None or msg.value() is None:
                            logger.error(f"Empty message in topic: {topic[0]}")
                            continue
                        if msg.error():
                            logger.error(f"Got error when trying to consume topic: {topic[0]}, error: {msg.error()}")
                            continue
                    except IndexError as e:
                        logger.error(f"No message found in topic: {topic[0]}")
                        continue
                    try:
                        schema['value'] = {"type":"json"}
                        schema['value']['data'] = json.loads(msg.value().decode())
                        logger.info(f"Message is json for topic {topic[0]}")
                    except json.JSONDecodeError as e:
                        logger.warning("Failed to parse message as json will default to string for topic: {}")
                        schema['value'] = {"type":"string"}
                        schema['value']['data'] = {'value':'string'}

                if self.schema_subjects and f"{topic[0]}-key" in self.schema_subjects:
                    schema['key'] = eval(self.schema_client.get_latest_version(f"{topic[0]}-key").schema.schema_str)
                    logger.info(f"Found  key schema in schema registry for topic: {topic[0]}")
                else:
                    logger.info(f"Did not find key schema in schema registry for topic: {topic[0]}, trying to pars as json")
                    try:
                        msg = self.consumer.consume(1, timeout=3)[0]
                        if msg is None or msg.key() is None:
                            logger.error(f"Empty message in topic: {topic[0]}")
                            continue
                        if msg.error():
                            logger.error(f"Got error when trying to consume topic: {topic[0]}, error: {msg.error()}")
                            continue
                    except IndexError as e:
                        logger.error(f"No message found in topic: {topic[0]}")
                        continue
                    try:
                        schema['key'] = {"type":"json"}
                        schema['key']['data'] = json.loads(msg.key().decode())
                        logger.info(f"Message key is json for topic {topic[0]}")
                    except json.JSONDecodeError as e:
                        logger.warning("Failed to parse key as json will default to string for topic: {}")
                        schema['key'] = {"type":"string"}
                        schema['key']['data'] = {'key':'string'}
                
                schemas.append(schema)
            return schemas

        except Exception as e:
            print("something wrong with the schemas!")





