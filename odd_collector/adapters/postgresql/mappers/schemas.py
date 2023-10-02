from funcy import lpluck_attr
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.models import Schema


def map_schema(
    generator: PostgresqlGenerator, schema: Schema, table_entities: list[DataEntity]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("schemas", schema.schema_name),
        name=schema.schema_name,
        type=DataEntityType.DATABASE_SERVICE,
        owner=None,
        metadata=[extract_metadata("postgres", schema, DefinitionType.DATASET)],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", table_entities)
        ),
    )
