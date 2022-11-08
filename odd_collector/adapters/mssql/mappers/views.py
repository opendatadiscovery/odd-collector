from functools import partial
from typing import List

from funcy import group_by, lmap
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import MssqlGenerator

from ..logger import logger
from ..models import View
from ..models.column import Column
from .columns import map_column
from .metadata import map_metadata
from .types import TABLE_TYPES_SQL_TO_ODD


def map_views(views: List[View], columns: List[Column], generator: MssqlGenerator):
    grouped_cols = group_by(lambda col: col.table_name, columns)

    for view in views:
        columns = grouped_cols.get(view.view_name)
        yield map_view(view, columns, generator)


def map_view(
    view: View, columns: List[Column], generator: MssqlGenerator
) -> DataEntity:
    schema: str = view.view_schema
    name: str = view.view_name

    generator.set_oddrn_paths(**{"schemas": schema, "views": name})

    oddrn = generator.get_oddrn_by_path("views")
    map_col = partial(map_column, oddrn_generator=generator, parent_oddrn_path="views")
    dataset = DataSet(field_list=lmap(map_col, columns))

    metadata = map_metadata(view)
    data_transformer = extract_data_transformer(view, generator)

    return DataEntity(
        oddrn=oddrn,
        name=name,
        type=DataEntityType.VIEW,
        metadata=metadata,
        dataset=dataset,
        data_transformer=data_transformer,
    )


def extract_data_transformer(view: View, generator: MssqlGenerator) -> DataTransformer:
    deps = view.view_dependencies

    inputs = []

    for dependency in deps:
        if _type := TABLE_TYPES_SQL_TO_ODD.get(dependency.table_type):
            path = "views" if _type == DataEntityType.VIEW else "tables"

            generator.set_oddrn_paths(
                **{"schemas": dependency.table_schema, path: dependency.table_name}
            )

            inputs.append(generator.get_oddrn_by_path(path=path))
        else:
            logger.error(f"Unsupported table type {dependency.table_type}")

    return DataTransformer(
        inputs=inputs,
        outputs=[],
    )
