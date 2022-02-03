from typing import Iterable

from odd_models.models import DataSetField, DataSetFieldType, Type

from .oddrn import ODDRN_BASE


class DatasetMapper:
    __sagemaker_types = {
        'Integral': Type.TYPE_INTEGER,
        'Fractional': Type.TYPE_NUMBER,
        'String': Type.TYPE_STRING
    }

    def __init__(self, region_name: str, aws_account_id: str) -> None:
        self.__aws_account_id = aws_account_id
        self.__region_name = region_name

    def map_feature_group_to_data_set_fields(self, feature_name: str, feature: dict) -> Iterable[DataSetField]:
        oddrn = (ODDRN_BASE + '/feature_groups/{feature_group_name}/features/{feature_name}').format(
            account_id=self.__aws_account_id,
            region_name=self.__region_name,
            feature_group_name=feature_name,
            feature_name=feature.get('FeatureName'))
        return DataSetField(
            oddrn=oddrn,
            name=feature.get('FeatureName'),
            type=DataSetFieldType(
                type=self.__sagemaker_types[feature.get('FeatureType')],
                logical_type=feature.get('FeatureType'),
                is_nullable=False,
            ),
        )
