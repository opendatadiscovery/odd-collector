from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MysqlGenerator


def map_database(
    generator: MysqlGenerator, database: str, entities: list[str]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        owner=None,
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(entities_list=entities),
    )
