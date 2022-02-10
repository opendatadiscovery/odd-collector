from abc import abstractmethod
import re
from typing import List, Optional

from oddrn_generator import KafkaGenerator


class BaseEngineParser:
    name = None
    settings = None

    @abstractmethod
    def __init__(self, source_table, settings):
        raise NotImplemented

    @abstractmethod
    def get_oddrns(self) -> List[str]:
        raise NotImplemented


class KafkaEngineParser(BaseEngineParser):
    name = "Kafka"

    def __init__(self, source_table, settings):
        self.table = source_table
        self.settings = settings

    def get_oddrns(self) -> List[str]:
        kafka_broker_list = self._get_kafka_settings(self.settings, "kafka_broker_list")
        kafka_topic_list = self._get_kafka_settings(self.settings, "kafka_topic_list")
        if not (kafka_broker_list and kafka_topic_list):
            return []
        kafka_oddrn_gen = KafkaGenerator(host_settings=kafka_broker_list)
        return [kafka_oddrn_gen.get_oddrn_by_path("topics", topic) for topic in kafka_topic_list.split(',')]

    def _get_kafka_settings(self, sql, pattern) -> Optional[str]:
        m = re.search(f"{pattern} = '(.+?)'", sql)
        if m:
            return m.group(1)
