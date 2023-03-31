import logging
from copy import deepcopy
from typing import List

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models import DataEntity, DataEntityType, DataSet
from odd_models.models import DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import PostgresqlGenerator

from ..models import Table
from .columns import map_column


def map_view(generator: PostgresqlGenerator, view: Table):
    data_entity_type = DataEntityType.VIEW
    generator.set_oddrn_paths(
        **{"schemas": view.table_schema, "views": view.table_name}
    )
    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=data_entity_type,
        owner=view.table_owner,
        description=view.description,
        metadata=[extract_metadata("postgres", view, DefinitionType.DATASET)],
        dataset=DataSet(
            rows_number=int(view.table_rows) if view.table_rows is not None else None,
            field_list=[map_column(c, generator, "views") for c in view.columns],
        ),
        data_transformer=extract_transformer_data(view.view_definition, generator),
    )


def extract_transformer_data(
    sql: str, oddrn_generator: PostgresqlGenerator
) -> DataTransformer:
    if sql is None:
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))

    try:
        inputs, outputs = sql_parser.get_response()
    except Exception:
        logging.error(f"Couldn't parse inputs and outputs from {sql}", exc_info=True)
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator),
        outputs=get_oddrn_list(outputs, oddrn_generator),
        sql=sql,
    )


def get_oddrn_list(tables, oddrn_generator: PostgresqlGenerator) -> List[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)
    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(oddrn_generator.get_oddrn_by_path("tables", table_name))
    return response
