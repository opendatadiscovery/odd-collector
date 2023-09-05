from copy import deepcopy

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import MysqlGenerator

from odd_collector.models import Table

from .columns import map_column


def map_view(generator: MysqlGenerator, table: Table) -> DataEntity:
    generator = deepcopy(generator)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views", table.name),
        name=table.name,
        type=DataEntityType.VIEW,
        owner=table.schema,
        description=table.comment,
        created_at=table.create_time.datetime,
        updated_at=table.update_time.datetime,
        metadata=[extract_metadata("mysql", table, DefinitionType.DATASET)],
        dataset=DataSet(
            rows_number=table.table_rows,
            field_list=[
                map_column(generator, column, "views") for column in table.columns
            ],
        ),
        data_transformer=DataTransformer(
            sql=table.sql_definition, inputs=[], outputs=[]
        ),
    )
