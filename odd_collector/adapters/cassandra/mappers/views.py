from copy import deepcopy
from typing import List

from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import CassandraGenerator

from odd_collector.adapters.cassandra.mappers.columns import map_column
from odd_collector.adapters.cassandra.mappers.models import ColumnMetadata, ViewMetadata


def map_views(
    generator: CassandraGenerator,
    views: List[ViewMetadata],
    columns: List[ColumnMetadata],
) -> List[DataEntity]:
    data_entities = []
    for view in views:
        generator.set_oddrn_paths(
            **{"keyspaces": view.keyspace_name, "views": view.view_name}
        )
        data_entity = DataEntity(
            oddrn=generator.get_oddrn_by_path("views"),
            name=view.view_name,
            type=DataEntityType.VIEW,
            description=view.comment,
            dataset=DataSet(
                field_list=[
                    map_column(col, "views_columns", generator, None)
                    for col in columns
                    if col.table_name == view.view_name
                ]
            ),
            data_transformer=extract_transformer_data(view.view_definition, generator),
        )
        data_entities.append(data_entity)
    return data_entities


def extract_transformer_data(
    sql: str, oddrn_generator: CassandraGenerator
) -> DataTransformer:
    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))
    inputs, outputs = sql_parser.get_response()
    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator),
        outputs=get_oddrn_list(outputs, oddrn_generator),
        sql=sql,
    )


def get_oddrn_list(tables, oddrn_generator: CassandraGenerator) -> List[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)
    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(oddrn_generator.get_oddrn_by_path("tables", table_name))
    return response
