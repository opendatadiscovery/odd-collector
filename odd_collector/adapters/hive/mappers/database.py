from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import HiveGenerator


def map_database(
    database: str, generator: HiveGenerator, tables_oddrn: list[str]
) -> DataEntity:
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        owner=None,
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=tables_oddrn),
    )
