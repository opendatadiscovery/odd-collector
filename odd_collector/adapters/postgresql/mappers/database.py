from funcy import lpluck_attr
from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import PostgresqlGenerator


def map_database(
    generator: PostgresqlGenerator, database: str, schema_entities: list[DataEntity]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schema_entities)
        ),
    )
