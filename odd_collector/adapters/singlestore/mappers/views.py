from copy import deepcopy

from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import SingleStoreGenerator

from ..logger import logger
from .columns import map_column
from .models import ColumnMetadata, TableMetadata
from .types import TABLE_TYPES_SQL_TO_ODD


def map_views(
    oddrn_generator: SingleStoreGenerator,
    views: list[tuple],
    columns: list[tuple],
    database: str,
) -> list[DataEntity]:
    data_entities: list[DataEntity] = []
    column_metadata = [ColumnMetadata(*c) for c in columns]
    oddrn_path = "views"

    for view in views:
        metadata: TableMetadata = TableMetadata(*view)
        table_name: str = metadata.table_name
        data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(
            metadata.table_type, DataEntityType.UNKNOWN
        )

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path, table_name),
            name=table_name,
            type=data_entity_type,
            owner=metadata.table_schema,
            description=metadata.table_comment,
            metadata=[],
        )
        data_entities.append(data_entity)

        # Dataset
        data_entity.dataset = DataSet(rows_number=metadata.table_rows, field_list=[])

        # DataTransformer
        if data_entity_type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(
                metadata.view_definition, oddrn_generator
            )

        for column in column_metadata:
            if column.table_name == table_name and column.table_schema == database:
                data_entity.dataset.field_list.append(
                    map_column(column, oddrn_generator, data_entity.owner, oddrn_path)
                )

    return data_entities


def extract_transformer_data(
    sql: str, oddrn_generator: SingleStoreGenerator
) -> DataTransformer:
    if type(sql) == bytes:
        sql = sql.decode("utf-8")
    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))

    try:
        inputs, outputs = sql_parser.get_response()
    except Exception as e:
        logger.error(f"Couldn't parse inputs and outputs from {sql}")
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator),
        outputs=get_oddrn_list(outputs, oddrn_generator),
        sql=sql,
    )


def get_oddrn_list(tables, oddrn_generator: SingleStoreGenerator) -> list[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)
    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(
            oddrn_generator.get_oddrn_by_path("tables", table_name.replace("`", ""))
        )
    return response
