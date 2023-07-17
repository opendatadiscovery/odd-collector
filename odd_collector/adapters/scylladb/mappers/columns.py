from typing import Union

from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import CassandraGenerator

from .models import ColumnMetadata
from ...cassandra.mappers.types import TYPES_CASSANDRA_TO_ODD


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

    oddrn_generator.set_oddrn_paths(**{column_path: name})
    dsf: DataSetField = DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path(column_path),
        name=name,
        owner=owner,
        metadata=[
            extract_metadata(
                "scylladb", column_metadata, DefinitionType.DATASET_FIELD, jsonify=True
            )
        ],
        type=DataSetFieldType(
            type=TYPES_CASSANDRA_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=data_type,
            is_nullable=column_metadata.kind != "partition_key",
        ),
        is_primary_key=column_metadata.kind in ["partition_key", "clustering"],
        is_sort_key=column_metadata.kind == "clustering",
    )

    return dsf
