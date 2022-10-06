from typing import Dict

from feast.entity import Entity
from feast.feature_view import FeatureView
from google.protobuf.json_format import MessageToDict
from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import FeastGenerator

from . import dataset_field_mapper, metadata_extractor


class FeatureViewMapper:
    def __init__(
        self,
        raw_feature_view_data: FeatureView,
        raw_entities_data_dict: Dict[str, Entity],
        oddrn_generator: FeastGenerator,
    ) -> None:
        self.__raw_feature_view_data = raw_feature_view_data
        self.__raw_entities_data_dict = raw_entities_data_dict
        self.__oddrn_generator = oddrn_generator

    def get_feature_view(self) -> DataEntity:
        self.__oddrn_generator.set_oddrn_paths(
            featureviews=self.__raw_feature_view_data.name
        )
        features = [
            dataset_field_mapper.map_entity(
                raw_entity_data=self.__raw_entities_data_dict.get(entity_name, {}),
                oddrn_generator=self.__oddrn_generator,
            )
            for entity_name in self.__raw_feature_view_data.entities
        ]
        features.extend(
            [
                dataset_field_mapper.map_feature(
                    raw_feature_data=f, oddrn_generator=self.__oddrn_generator
                )
                for f in self.__raw_feature_view_data.features
            ]
        )
        return DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("featureviews"),
            type=DataEntityType.FEATURE_GROUP,
            name=self.__raw_feature_view_data.name,
            owner=None,
            description=None,
            updated_at=self.__raw_feature_view_data.last_updated_timestamp,
            created_at=self.__raw_feature_view_data.created_timestamp,
            metadata=[
                metadata_extractor.extract_dataset_metadata(
                    MessageToDict(self.__raw_feature_view_data.to_proto())
                )
            ],
            dataset=DataSet(
                parent_oddrn=None, rows_number=0, field_list=list(flatten(features))
            ),
        )
