from functools import partial

from funcy import lmap
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import MssqlGenerator

from ..logger import logger
from ..models import View
from .columns import map_column
from .metadata import dataset_metadata
from .types import TABLE_TYPES_SQL_TO_ODD


def map_view(view: View, generator: MssqlGenerator) -> DataEntity:
    schema: str = view.view_schema
    name: str = view.view_name

    generator.set_oddrn_paths(**{"schemas": schema, "views": name})

    oddrn = generator.get_oddrn_by_path("views")
    map_col = partial(map_column, oddrn_generator=generator, parent_oddrn_path="views")
    dataset = DataSet(field_list=lmap(map_col, view.columns))

    return DataEntity(
        oddrn=oddrn,
        name=name,
        owner=None,
        type=DataEntityType.VIEW,
        metadata=[dataset_metadata(entity=view)],
        dataset=dataset,
        data_transformer=extract_data_transformer(view, generator),
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
