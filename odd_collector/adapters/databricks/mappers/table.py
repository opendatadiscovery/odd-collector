from oddrn_generator import DatabricksUnityCatalogGenerator
from odd_models.models import DataEntityType, DataSet
from odd_models.models import DataEntity
from .column import map_column


def map_table(
    oddrn_generator: DatabricksUnityCatalogGenerator,
    table_metadata: dict,
) -> DataEntity:
    table_name = table_metadata["name"]
    columns_nodes = table_metadata["columns"]
    oddrn_generator.set_oddrn_paths(
        catalogs=table_metadata["catalog_name"],
        schemas=table_metadata["schema_name"],
        tables=table_name,
    )
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table_name),
        name=table_name,
        type=DataEntityType.TABLE,
        owner=table_metadata["owner"],
        created_at=table_metadata["created_at"],
        updated_at=table_metadata["updated_at"],
        metadata=[],
        dataset=DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            field_list=[
                map_column(oddrn_generator, column_node)
                for column_node in columns_nodes
            ],
        ),
    )
