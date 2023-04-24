from copy import deepcopy
from typing import Optional

from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import MysqlGenerator

from odd_collector.logger import logger
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
        data_transformer=extract_transformer_data(
            sql=table.sql_definition, generator=generator
        ),
    )


def extract_transformer_data(
    generator: MysqlGenerator, sql: Optional[str] = None
) -> DataTransformer:
    if not sql:
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    if type(sql) == bytes:
        sql = sql.decode("utf-8")
    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))

    try:
        inputs, outputs = sql_parser.get_response()
    except Exception as e:
        logger.warning(f"Couldn't parse inputs and outputs from {sql}")
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    return DataTransformer(
        inputs=get_oddrn_list(inputs, generator),
        outputs=get_oddrn_list(outputs, generator),
        sql=sql,
    )


def get_oddrn_list(tables, generator: MysqlGenerator) -> list[str]:
    response = []
    generator = deepcopy(generator)

    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(
            generator.get_oddrn_by_path("tables", table_name.replace("`", ""))
        )
    return response
