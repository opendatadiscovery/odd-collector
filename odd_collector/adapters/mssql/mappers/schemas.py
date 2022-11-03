from collections import defaultdict
from typing import List

from odd_models.models import DataEntity, DataEntityGroup, DataEntityType
from oddrn_generator import MssqlGenerator

from ..models import Table, View


def map_schemas(
    tables: List[Table],
    views: List[View],
    generator: MssqlGenerator,
):
    schemas = defaultdict(set)

    for table in tables:
        schema_name = table.table_schema
        generator.set_oddrn_paths(schemas=schema_name, tables=table.table_name)
        schemas[schema_name].add(generator.get_oddrn_by_path("tables"))

    for view in views:
        schema_name = view.view_schema
        generator.set_oddrn_paths(schemas=schema_name, views=view.view_name)
        schemas[schema_name].add(generator.get_oddrn_by_path("views"))

    for schema_name, values in schemas.items():
        oddrn = generator.get_oddrn_by_path("schemas", schema_name)

        yield DataEntity(
            type=DataEntityType.DATABASE_SERVICE,
            name=schema_name,
            oddrn=oddrn,
            data_entity_group=DataEntityGroup(entities_list=list(values)),
        )
