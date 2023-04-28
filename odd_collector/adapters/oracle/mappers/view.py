from typing import Iterable, List, Optional

from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import OracleGenerator

from ..domain import Dependency, DependencyType, View
from .column import map_column


def map_view(generator: OracleGenerator, view: View) -> DataEntity:
    generator.set_oddrn_paths(views=view.name)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.name,
        type=DataEntityType.VIEW,
        description=view.description,
        dataset=DataSet(
            field_list=[
                map_column(generator, "views_columns", column)
                for column in view.columns
            ]
        ),
        data_transformer=DataTransformer(
            inputs=list(_map_dependency(generator, view.upstream)),
            outputs=list(_map_dependency(generator, view.downstream)),
        ),
    )


def _map_dependency(
    generator: OracleGenerator, deps: Optional[List[Dependency]]
) -> Iterable[str]:
    for dependency in deps:
        if dependency.referenced_type == DependencyType.TABLE:
            yield generator.get_oddrn_by_path("tables", dependency.referenced_name)
        elif dependency.referenced_type == DependencyType.VIEW:
            yield generator.get_oddrn_by_path("views", dependency.referenced_name)
