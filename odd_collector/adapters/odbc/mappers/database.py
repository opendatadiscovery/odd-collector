from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator.generators import OdbcGenerator


def map_database(generator: OdbcGenerator, db_name: str) -> DataEntity:
    return DataEntity(
        name=db_name,
        oddrn=generator.get_oddrn_by_path("databases"),
        type=DataEntityType.DATABASE_SERVICE,
        data_entity_group=DataEntityGroup(entities_list=[]),
    )
