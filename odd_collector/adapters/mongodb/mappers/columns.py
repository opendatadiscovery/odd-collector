from odd_models.models import DataSetField, DataSetFieldType, Type
from .types import TYPES_MONGO_TO_ODD

def map_column(
        column_metadata, oddrn_generator,
        parent_oddrn_path: str
) -> DataSetField:
    name: str = column_metadata["title"]
    data_type: str = column_metadata["bsonType"]
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(f'columns', name),
        name=name,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_MONGO_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            is_nullable=column_metadata["bsonType"] != 'partition_key'
        )
    )

    return dsf
