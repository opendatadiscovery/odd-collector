from typing import List

from funcy import first, partial
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import TableauGenerator

from ..domain.table import Table
from . import DATA_SET_EXCLUDED_KEYS, DATA_SET_SCHEMA
from .columns import map_column
from .metadata import extract_metadata

extract_metadata = partial(
    extract_metadata, schema_url=DATA_SET_SCHEMA, excluded_key=DATA_SET_EXCLUDED_KEYS
)


def map_table(oddrn_generator: TableauGenerator, table: Table) -> DataEntity:
    # TODO: Now table model doesn't have metadata field, need to add it
    metadata = extract_metadata(metadata={})
    # Each database has multiple owners, by odd specification we can attach only 1 owner
    # take first owner or None
    owner = first(table.owners)
    try:
        return DataEntity(
            oddrn=table.get_oddrn(oddrn_generator),
            name=table.name,
            owner=owner,
            metadata=metadata,
            description=table.description,
            type=DataEntityType.TABLE,
            dataset=create_dataset(oddrn_generator, table),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping table {table.name} failed") from e


def map_tables(
    oddrn_generator: TableauGenerator, tables: List[Table]
) -> List[DataEntity]:
    return [map_table(oddrn_generator, table) for table in tables]


def create_dataset(oddrn_generator, table: Table):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("tables")
    columns = [map_column(oddrn_generator, column) for column in table.columns]

    return DataSet(
        parent_oddrn=parent_oddrn,
        field_list=columns,
    )
