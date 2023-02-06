from collections import defaultdict
from typing import Dict, List

from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import CassandraGenerator

from .columns import map_column
from .metadata import get_metadata_extension
from .models import ColumnMetadata, TableMetadata

_data_set_metadata_excluded_keys: set = set()
_data_set_metadata_schema_url: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/"
    "opendatadiscovery-specification/main/specification/extensions/cassandra.json#/"
    "definitions/CassandraDataSetExtension"
)


def get_table_name_to_columns(
    columns: List[ColumnMetadata],
) -> Dict[str, List[ColumnMetadata]]:
    """
    A method to transform the format of the columns from tuple to a dictionary where the key is the table name, and the
    value is a list of the columns associated with the said table name.
    :param columns: a tuple of columns.
    :return: a dictionary where the key is the table name, and the value is a list of the columns associated with the
    said table name.
    """
    table_name_to_columns = defaultdict(list)

    for column in columns:
        table_name_to_columns[column.table_name].append(column)

    return table_name_to_columns


def get_dataset(
    table_columns: List[ColumnMetadata], oddrn_generator: CassandraGenerator
) -> DataSet:
    """
    A method to create a dataset of a particular table given a list of its columns and the database's generator.
    :param table_columns: a list of the table's columns.
    :param oddrn_generator: the database generator.
    :return: a DataSet of the table.
    """
    dataset = DataSet(
        field_list=[
            map_column(column, "tables_columns", oddrn_generator, None)
            for column in table_columns
        ]
    )
    return dataset


def get_data_entity(
    metadata: TableMetadata,
    oddrn_generator: CassandraGenerator,
    keyspace: str,
    table_columns: List[ColumnMetadata],
) -> DataEntity:
    """
    A method to get the data entity of a particular table. It generates the necessary information like the table's data
    set and its metadata, etc.
    :param metadata: the table's information.
    :param oddrn_generator: the database generator.
    :param keyspace: the name of the keyspace where the table is located.
    :param table_columns: a list of the columns in the table.
    :return: a data entity of the table.
    """
    data_entity_type = DataEntityType.TABLE
    oddrn_path = "tables"
    table_name: str = metadata.table_name

    oddrn_generator.set_oddrn_paths(**{"keyspaces": keyspace, "tables": table_name})
    metadata_extension = get_metadata_extension(
        _data_set_metadata_schema_url, metadata, _data_set_metadata_excluded_keys
    )
    dataset = get_dataset(table_columns, oddrn_generator)
    data_entity: DataEntity = DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
        name=table_name,
        type=data_entity_type,
        metadata=[metadata_extension] if metadata_extension else [],
        dataset=dataset,
    )

    return data_entity


def map_tables(
    oddrn_generator: CassandraGenerator,
    tables: List[TableMetadata],
    columns: List[ColumnMetadata],
    keyspace: str,
) -> List[DataEntity]:
    """
    A method to map tables in a keyspace to a list of data entities. This is done by filtering the data, and then
    generating a data entity for each of the tables given. And finally by adding a data entity for the whole keyspace.
    :param oddrn_generator: the database generator.
    :param tables: a list of the tables.
    :param columns: a list of the columns.
    :param keyspace: the name of the keyspace.
    :return: a list of data entities describing the tables given.
    """
    data_entities: List[DataEntity] = []

    table_name_to_columns: Dict[str, List[ColumnMetadata]] = get_table_name_to_columns(
        columns
    )

    for table in tables:
        table_columns = table_name_to_columns.get(table.table_name, [])
        data_entity = get_data_entity(table, oddrn_generator, keyspace, table_columns)
        data_entities.append(data_entity)

    return data_entities
