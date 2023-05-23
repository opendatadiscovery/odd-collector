from funcy import lpluck_attr
from oddrn_generator import DatabricksUnityCatalogGenerator
from odd_models.models import DataEntityGroup, DataEntityType, DataEntity


def map_catalog(
    oddrn_generator: DatabricksUnityCatalogGenerator,
    catalog_name: str,
    schemas_entities: list[DataEntity],
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("catalogs", catalog_name),
        name=catalog_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schemas_entities)
        ),
    )
