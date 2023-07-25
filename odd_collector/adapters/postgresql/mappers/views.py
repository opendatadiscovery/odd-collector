from funcy import lmap, partial, silent
from odd_models import DataEntity, DataEntityType, DataSet
from odd_models.models import DataTransformer
from oddrn_generator import PostgresqlGenerator

from ..models import Table
from .columns import map_column
from .metadata import get_table_metadata


def map_view(generator: PostgresqlGenerator, view: Table):
    generator.set_oddrn_paths(
        **{"schemas": view.table_schema, "views": view.table_name}
    )
    map_view_column = partial(map_column, generator=generator, path="views")

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=DataEntityType.VIEW,
        owner=view.table_owner,
        description=view.description,
        metadata=[get_table_metadata(entity=view)],
        dataset=DataSet(
            rows_number=silent(int)(view.table_rows),
            field_list=lmap(map_view_column, view.columns),
        ),
        data_transformer=DataTransformer(
            sql=view.view_definition, inputs=[], outputs=[]
        ),
    )
