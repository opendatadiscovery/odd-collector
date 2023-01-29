from odd_models.models import DataEntity, DataEntityType, DataEntityGroup
from oddrn_generator import HiveGenerator


def map_database(database: str, generator: HiveGenerator, tables_oddrn: list[str]):
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("databases"),
        name=database,
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=tables_oddrn),
    )
