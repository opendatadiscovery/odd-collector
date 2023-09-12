from funcy import lpluck_attr
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from odd_models.models import DataEntity, DataEntityGroup, DataEntityType, DataSet
from oddrn_generator import RedshiftGenerator

from .metadata import MetadataSchema


def map_schema(
    generator: RedshiftGenerator,
    schema: MetadataSchema,
    table_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("schemas", schema.schema_name),
        name=schema.schema_name,
        owner=schema.base.schema_owner,
        metadata=[extract_metadata("redshift", schema, DefinitionType.DATASET)],
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", table_entities)
        ),
    )
