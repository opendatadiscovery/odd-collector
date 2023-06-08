from dataclasses import dataclass
from typing import Optional

from ..grammar_parser.parser import parser
from ..grammar_parser.transformer import transformer
from ..logger import logger
from .column_statistics import ColumnStatistics
from .column_types import ColumnType, UnionColumnType


@dataclass
class Column:
    name: str
    type: ColumnType
    comment: Optional[str] = None
    statistics: Optional[ColumnStatistics] = None


def parse_column_type(column_type: str) -> ColumnType:
    try:
        parsed = parser.parse(column_type)
        col_type: ColumnType = transformer.transform(parsed)
        col_type.logical_type = column_type

        return col_type
    except Exception as e:
        logger.error(f"Could not parse type {column_type}. {e}")
        return UnionColumnType(logical_type=column_type)
