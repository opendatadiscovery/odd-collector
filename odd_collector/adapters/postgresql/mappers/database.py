from odd_models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import PostgresqlGenerator


def map_database(
    generator: PostgresqlGenerator, database: str, entities: list[str]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
