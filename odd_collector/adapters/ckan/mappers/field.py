from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import CKANGenerator

from .models import ResourceField
from .types import TYPES_SQL_TO_ODD


def map_field(
    oddrn_generator: CKANGenerator,
    field: ResourceField,
) -> DataSetField:
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("fields", field.name),
        name=field.name,
        owner=None,
        metadata=[extract_metadata("ckan", field, DefinitionType.DATASET_FIELD)],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(field.type, Type.TYPE_UNKNOWN),
            logical_type=field.type,
            is_nullable=field.is_nullable,
        ),
    )
