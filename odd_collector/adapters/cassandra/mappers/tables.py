from collections import defaultdict

from odd_models.models import DataEntity, DataSet, DataEntityType, DataEntityGroup

from typing import List, Dict, Tuple, Any, Union

from . import (
    TableMetadata,
    ColumnMetadata,
    _data_set_metadata_schema_url,
    _data_set_metadata_excluded_keys,
)
from .columns import map_column
from .metadata import get_metadata_extension

from ..cassandra_generator import CassandraGenerator

from cassandra.util import OrderedMapSerializedKey, SortedSet


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
            map_column(column, oddrn_generator, None) for column in table_columns
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


def filter_data(data: Tuple[Any]) -> Tuple[Any]:
    """
    A method to filter the data obtained from the Cassandra database. It converts the Cassandra types
    OrderedMapSerializedKey, SortedSet to usual Python dictionary and list, respectively
    :param data: the data obtained from the Cassandra database.
    :return: the same data after filtering the types.
    """
    filtered = []
    for value in data:
        if type(value) is OrderedMapSerializedKey:
            filtered.append(dict(value))
        elif type(value) is SortedSet:
            filtered.append(list(value))
        else:
            filtered.append(value)
    return tuple(filtered)


def map_tables(
    oddrn_generator: CassandraGenerator,
    tables: List[Tuple],
    columns: List[Tuple],
    keyspace: str,
) -> List[DataEntity]:
    """
    A method to map tables in a keyspace to a list of data entities. This is done by filtering the data, and then
    generating a data entity for each of the tables given. And finally by adding a data entity for the whole keyspace.
    :param oddrn_generator: the database generator.
    :param tables: a tuple of the tables.
    :param columns: a tuple of the columns.
    :param keyspace: the name of the keyspace.
    :return: a list of data entities describing the tables given.
    """
    data_entities: List[DataEntity] = []

    tables = [filter_data(table) for table in tables]
    columns = [filter_data(column) for column in columns]

    tables = [TableMetadata(*table) for table in tables]
    columns = [ColumnMetadata(*column) for column in columns]

    table_name_to_columns: Dict[str, List[ColumnMetadata]] = get_table_name_to_columns(
        columns
    )

    for table in tables:
        table_columns = table_name_to_columns.get(table.table_name, [])
        data_entity = get_data_entity(table, oddrn_generator, keyspace, table_columns)
        data_entities.append(data_entity)

    data_entities.append(
        DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path("keyspaces"),
            name=keyspace,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[de.oddrn for de in data_entities]
            ),
        )
    )

    return data_entities
