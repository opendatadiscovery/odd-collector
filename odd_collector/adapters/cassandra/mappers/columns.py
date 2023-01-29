from typing import Union

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import CassandraGenerator

from .metadata import get_metadata_extension
from .models import ColumnMetadata
from .types import TYPES_CASSANDRA_TO_ODD

_data_set_field_metadata_excluded_keys: set = set()
_data_set_field_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/"
    "opendatadiscovery-specification/main/specification/extensions/cassandra.json"
    "#/definitions/CassandraDataSetFieldExtension"
)


def map_column(
    column_metadata: ColumnMetadata,
    column_path: str,
    oddrn_generator: CassandraGenerator,
    owner: Union[str, None],
) -> DataSetField:
    """
    A method to map a column to a dataset field. It extracts the necessary information and generates the required
    dataset field.
    :param column_metadata: the column's information like name, type, etc.
    :param column_path: parent type 'tables_column' | 'views_column'
    :param oddrn_generator: the database generator.
    :param owner: the owner of the column.
    :return:
    """
    name: str = column_metadata.column_name
    data_type: str = column_metadata.type

    metadata_extension = get_metadata_extension(
        _data_set_field_metadata_schema_url,
        column_metadata,
        _data_set_field_metadata_excluded_keys,
    )
    oddrn_generator.set_oddrn_paths(**{column_path: name})
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(column_path),
        name=name,
        owner=owner,
        metadata=[metadata_extension] if metadata_extension else [],
        type=DataSetFieldType(
            type=TYPES_CASSANDRA_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=data_type,
            is_nullable=column_metadata.kind != "partition_key",
        ),
        is_primary_key=column_metadata.kind in ["partition_key", "clustering"],
        is_sort_key=column_metadata.kind == "clustering",
    )

    return dsf
