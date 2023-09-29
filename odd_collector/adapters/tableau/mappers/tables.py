from funcy import partial
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import TableauGenerator

from ..domain.table import EmbeddedTable
from . import DATA_SET_EXCLUDED_KEYS, DATA_SET_SCHEMA
from .columns import map_column
from .metadata import extract_metadata

extract_metadata = partial(
    extract_metadata, schema_url=DATA_SET_SCHEMA, excluded_key=DATA_SET_EXCLUDED_KEYS
)


def map_table(generator: TableauGenerator, table: EmbeddedTable) -> DataEntity:
    # TODO: Now table model doesn't have metadata field, need to add it
    metadata = extract_metadata(metadata={})
    # Each database has multiple owners, by odd specification we can attach only 1 owner
    # take first owner or None
    owner = None
    generator.set_oddrn_paths(
        databases=table.db_id,
        schemas=table.schema or "unknown_schema",
        tables=table.id,
    )
    try:
        return DataEntity(
            oddrn=generator.get_oddrn_by_path("tables"),
            name=table.name,
            owner=owner,
            metadata=metadata,
            description=table.description,
            type=DataEntityType.TABLE,
            dataset=create_dataset(generator, table),
        )
    except Exception as e:
        raise MappingDataError(f"Mapping table {table.name} failed") from e


def create_dataset(oddrn_generator, table: EmbeddedTable):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("tables")
    columns = [map_column(oddrn_generator, column) for column in table.columns]

    return DataSet(
        parent_oddrn=parent_oddrn,
        field_list=columns,
    )
