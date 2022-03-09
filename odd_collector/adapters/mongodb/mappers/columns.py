from itsdangerous import NoneAlgorithm
from odd_models.models import DataSetField, DataSetFieldType, Type
from typing import List
from .types import TYPES_MONGO_TO_ODD



def map_columns(data: dict, oddrn_generator)->List[DataSetField]:
    collector = []
    __map_columns(collector, data, oddrn_generator)
    return collector


def __map_columns(collector: list, data: dict, oddrn_generator, parent_oddrn: str = None)->List[DataSetField]:
    for key,value in data.items():  
        metadata = {"title": key, "bsonType": type(value).__name__}   
        column = __map_column(metadata,oddrn_generator, parent_oddrn)
        collector.append(column)
        if isinstance(value, dict):
            __map_columns(collector, value, oddrn_generator, column.oddrn)
        if isinstance(value, list) and len(value)>0:
             __map_columns(collector, {'Values':value[0]}, oddrn_generator, column.oddrn)


def __map_column(
        column_metadata, oddrn_generator,
        parent_oddrn: str = None 
) -> DataSetField:
    name: str = column_metadata["title"]
    parent_field_oddrn = parent_oddrn if parent_oddrn else None
    oddrn = oddrn_generator.get_oddrn_by_path(f'columns', name)  if parent_oddrn is None else f'{parent_field_oddrn}/subcolumns/{name}'
    data_type: str = column_metadata["bsonType"]
    dsf = DataSetField(
        oddrn=oddrn,
        name=name,
        parent_field_oddrn=parent_field_oddrn,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_MONGO_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            is_nullable=column_metadata["bsonType"] != 'partition_key',
            logical_type = column_metadata["bsonType"] 
        )
    )

    return dsf
