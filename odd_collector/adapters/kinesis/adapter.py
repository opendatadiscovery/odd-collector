import boto3

from typing import List
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import KinesisGenerator
from .mappers.streams import map_kinesis_stream


class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        self._kinesis_client = boto3.client(
            'kinesis',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            region_name=config.region_name)

        self.__oddrn_generator = KinesisGenerator(
            cloud_settings={"region": config.region_name,
                            "account": config.account_id}
        )

    def get_data_entity_list(self) -> DataEntityList:
        data_entities = self.get_data_entities()
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=data_entities,
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        retrieve streams and all their metadata and converting them to DataEntity type
        :return: list of data entities (streams)
        """
        streams = self._kinesis_client.list_streams()['StreamNames']

        return [map_kinesis_stream(self._kinesis_client.describe_stream(StreamName=stream)["StreamDescription"],
                                   self.__oddrn_generator) for stream in streams]
