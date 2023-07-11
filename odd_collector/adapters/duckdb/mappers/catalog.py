from dataclasses import dataclass
from funcy import lpluck_attr
from pathlib import Path
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from oddrn_generator import DuckDBGenerator
from odd_models.models import DataEntityGroup, DataEntityType, DataEntity


@dataclass
class CatalogMetadata:
    odd_metadata: dict


def map_catalog(
    oddrn_generator: DuckDBGenerator,
    catalog_name: str,
    schemas_entities: list[DataEntity],
    db_file_path: Path,
) -> DataEntity:
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("catalogs", catalog_name),
        name=catalog_name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[
            extract_metadata(
                "duckdb",
                CatalogMetadata({"database_file_path": db_file_path}),
                DefinitionType.DATASET,
            )
        ],
        data_entity_group=DataEntityGroup(
            entities_list=lpluck_attr("oddrn", schemas_entities)
        ),
    )
