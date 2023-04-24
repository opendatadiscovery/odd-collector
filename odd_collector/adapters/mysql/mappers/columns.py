from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import MysqlGenerator

from odd_collector.models import Column

TYPES_SQL_TO_ODD: dict[str, Type] = {
    "tinyint": Type.TYPE_INTEGER,
    "smallint": Type.TYPE_INTEGER,
    "mediumint": Type.TYPE_INTEGER,
    "int": Type.TYPE_INTEGER,
    "integer": Type.TYPE_INTEGER,
    "bigint": Type.TYPE_INTEGER,
    "float": Type.TYPE_NUMBER,
    "real": Type.TYPE_NUMBER,
    "double": Type.TYPE_NUMBER,
    "double precision": Type.TYPE_NUMBER,
    "decimal": Type.TYPE_NUMBER,
    "numeric": Type.TYPE_NUMBER,
    "bit": Type.TYPE_BINARY,
    "boolean": Type.TYPE_BOOLEAN,
    "char": Type.TYPE_CHAR,
    "varchar": Type.TYPE_STRING,
    "tinytext": Type.TYPE_STRING,
    "mediumtext": Type.TYPE_STRING,
    "longtext": Type.TYPE_STRING,
    "text": Type.TYPE_STRING,
    "interval": Type.TYPE_DURATION,
    "date": Type.TYPE_DATETIME,
    "time": Type.TYPE_DATETIME,
    "datetime": Type.TYPE_DATETIME,
    "timestamp": Type.TYPE_DATETIME,
    "year": Type.TYPE_INTEGER,
    "binary": Type.TYPE_BINARY,
    "varbinary": Type.TYPE_BINARY,
    "tinyblob": Type.TYPE_BINARY,
    "mediumblob": Type.TYPE_BINARY,
    "longblob": Type.TYPE_BINARY,
    "blob": Type.TYPE_BINARY,
    "json": Type.TYPE_STRING,
    "enum": Type.TYPE_UNION,
    "set": Type.TYPE_LIST,
}


def map_column(
    generator: MysqlGenerator,
    column: Column,
    oddrn_path: str,
) -> DataSetField:
    return DataSetField(
        oddrn=generator.get_oddrn_by_path(f"{oddrn_path}_columns", column.name),
        name=column.name,
        owner=None,
        metadata=[extract_metadata("mysql", column, DefinitionType.DATASET_FIELD)],
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(column.type, Type.TYPE_UNKNOWN),
            logical_type=column.type,
            is_nullable=column.is_nullable == "YES",
        ),
        default_value=str(column.default),
        description=column.comment,
        is_primary_key=False,
    )
