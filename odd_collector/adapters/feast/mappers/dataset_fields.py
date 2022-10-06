from typing import List

from feast.entity import Entity
from feast.feature import Feature
from feast.value_type import ValueType
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import FeastGenerator

from .types import TYPES_FEAST_FEATURE_TO_ODD


class DatasetFieldMapper:
    def map_feature(
        self, raw_feature_data: Feature, oddrn_generator: FeastGenerator
    ) -> List[DataSetField]:
        return self.__extract_dataset_fields(
            name=raw_feature_data.name,
            logical_type=raw_feature_data.dtype,
            description=", ".join(
                ": ".join(label_pair) for label_pair in raw_feature_data.labels.items()
            ),
            oddrn_generator=oddrn_generator,
        )

    def map_entity(
        self, raw_entity_data: Entity, oddrn_generator: FeastGenerator
    ) -> List[DataSetField]:
        return self.__extract_dataset_fields(
            name=raw_entity_data.name,
            logical_type=raw_entity_data.value_type,
            description=raw_entity_data.description,
            oddrn_generator=oddrn_generator,
        )

    def __extract_dataset_fields(
        self,
        name: str,
        logical_type: ValueType,
        description: str,
        oddrn_generator: FeastGenerator,
    ) -> List[DataSetField]:
        result = []

        odd_type = self.__map_feature_type(logical_type=logical_type)
        parent_oddrn = oddrn_generator.get_oddrn_by_path("features", name)
        result.append(
            self.__map_dataset_field(
                oddrn=parent_oddrn,
                name=name,
                logical_type=logical_type,
                odd_type=odd_type,
                description=description,
            )
        )

        child_logical_type = TYPES_FEAST_FEATURE_TO_ODD.get(logical_type.value, {}).get(
            "child", None
        )
        if child_logical_type:
            child_odd_type = self.__map_feature_type(logical_type=child_logical_type)
            child_name = self.__map_subfeature_name(child_odd_type)
            child_oddrn = oddrn_generator.get_oddrn_by_path("subfeatures", child_name)

            result.append(
                self.__map_dataset_field(
                    oddrn=child_oddrn,
                    parent_oddrn=parent_oddrn,
                    name=child_name,
                    logical_type=child_logical_type,
                    odd_type=child_odd_type,
                )
            )
        return result

    def __map_dataset_field(
        self,
        oddrn: str,
        name: str,
        logical_type: ValueType,
        odd_type: str,
        description: str = None,
        parent_oddrn: str = None,
    ) -> DataSetField:
        return DataSetField(
            oddrn=oddrn,
            parent_field_oddrn=parent_oddrn,
            name=name,
            type=DataSetFieldType(
                type=odd_type, logical_type=str(logical_type), is_nullable=True
            ),
            description=description,
        )

    def __map_subfeature_name(self, odd_type: str):
        return str(odd_type).replace("Type.TYPE_", "").lower()

    def __map_feature_type(self, logical_type: ValueType):
        return TYPES_FEAST_FEATURE_TO_ODD.get(logical_type.value, {}).get(
            "field_type", Type.TYPE_UNKNOWN
        )
