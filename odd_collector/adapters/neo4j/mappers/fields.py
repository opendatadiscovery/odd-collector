from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import Neo4jGenerator

from . import FieldMetadata
from .metadata import convert_bytes_to_str
from .types import TYPES_SQL_TO_ODD


def map_field(
    field: FieldMetadata, oddrn_generator: Neo4jGenerator, owner: str
) -> DataSetField:
    name: str = field.field_name
    data_type: str = convert_bytes_to_str(field.data_type)
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("fields", name),
        name=name,
        owner=owner,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            is_nullable=True,
            logical_type=convert_bytes_to_str(field.data_type),
        ),
        default_value="",
        description=None,
    )

    return dsf
