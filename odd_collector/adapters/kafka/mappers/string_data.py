from attr import field
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import Generator
from typing import Dict, List
from .types import TYPES_MONGO_TO_ODD


def string_data(data: dict, oddrn_generator: Generator)->List[DataSetField]:

    fields = []
    for key,value in data.items():  
        dsf = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(f'columns', key) ,
        name=key,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_MONGO_TO_ODD.get(value['type'], Type.TYPE_UNKNOWN),
            is_nullable = True
            )
        )
        fields.append(dsf)

    return fields
