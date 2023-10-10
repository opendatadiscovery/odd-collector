from funcy import partial
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import TableauGenerator

from ..domain.column import Column
from . import DATA_SET_FIELD_EXCLUDED_KEYS, DATA_SET_FIELD_SCHEMA
from .metadata import extract_metadata
from .types import map_type

extract_metadata = partial(
    extract_metadata,
    schema_url=DATA_SET_FIELD_SCHEMA,
    excluded_key=DATA_SET_FIELD_EXCLUDED_KEYS,
)


def map_column(generator: TableauGenerator, column: Column) -> DataSetField:
    column_name = column.name
    column_type = column.remote_type

    # TODO: Add metadata
    metadata = {}
    try:
        return DataSetField(
            oddrn=generator.get_oddrn_by_path("columns", column_name),
            name=column_name,
            metadata=extract_metadata(metadata=metadata),
            owner=None,
            type=DataSetFieldType(
                type=map_type(column_type),
                logical_type=column_type,
                is_nullable=column.is_nullable,
            ),
            description=column.description,
        )
    except Exception as e:
        raise MappingDataError(f"Mapping column {column_name} failed") from e
