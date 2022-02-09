from typing import Iterable

import boto3
import pytz
from odd_models.models import DataEntity, DataEntityType, DataSet, List
from odd_models.models import DataEntityList
from odd_aws_collector.domain.plugin import SagemakerPlugin
from odd_aws_collector.domain.adapter import AbstractAdapter

from .mappers.datasets import DatasetMapper
from .mappers.metadata import metadata_extractor
from .mappers.oddrn import ODDRN_BASE


class Adapter(AbstractAdapter):

    def __init__(self, config: SagemakerPlugin) -> None:
        self.__sagemaker_client = boto3.client('sagemaker',
                                               aws_access_key_id=config.aws_access_key_id,
                                               aws_secret_access_key=config.aws_secret_access_key,
                                               region_name=config.aws_region,
                                               )
        self.__aws_account_id = boto3.client("sts",
                                             aws_access_key_id=config.aws_access_key_id,
                                             aws_secret_access_key=config.aws_secret_access_key).get_caller_identity()[
            "Account"]
        self.__region_name = config.aws_region
        self.__dataset_mapper = DatasetMapper(self.__region_name, self.__aws_account_id)

    def get_data_source_oddrn(self) -> str:
        return self._oddrn_generator.get_data_source_oddrn()
    """
    def get_datasets(self) -> Iterable[DataEntity]:
        return self.__fetch_feature_groups()
    """

    def get_data_entities(self) -> Iterable[DataEntity]:
        return self.__fetch_feature_groups()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn= (ODDRN_BASE).format(account_id=self.__aws_account_id,
                                                                             region_name=self.__region_name),
            items=list(self.get_data_entities()),
        )

    def get_transformers(self) -> List[DataEntity]:
        return []

    def get_transformers_runs(self) -> List[DataEntity]:
        return []

    def __fetch_feature_groups(self) -> Iterable[DataEntity]:
        feature_groups_list = self.__sagemaker_client.list_feature_groups().get('FeatureGroupSummaries')
        return [self.__map_feature_group_to_data_entity(f) for f in
                [self.__fetch_feature_group(fg) for fg in feature_groups_list]]

    def __fetch_feature_group(self, feature: dict) -> dict:
        return self.__sagemaker_client.describe_feature_group(
            FeatureGroupName=feature.get('FeatureGroupName')
        )

    def __map_feature_group_to_data_entity(self, feature: dict) -> DataEntity:
        oddrn = (ODDRN_BASE + '/feature_groups/{feature_group_name}').format(account_id=self.__aws_account_id,
                                                                             region_name=self.__region_name,
                                                                             feature_group_name=feature.get(
                                                                                 'FeatureGroupName'))
        return DataEntity(
            oddrn=oddrn,
            name=feature.get('FeatureGroupName'),
            type=DataEntityType.FEATURE_GROUP,
            metadata=metadata_extractor.extract_dataset_metadata(feature),
            created_at=feature.get('CreationTime').astimezone(pytz.utc),
            dataset=DataSet(
                field_list=
                [self.__dataset_mapper.map_feature_group_to_data_set_fields(feature.get('FeatureGroupName'), f) for f in
                 feature.get('FeatureDefinitions')],
            )
        )
