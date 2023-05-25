import traceback
from datetime import datetime
from typing import Callable, Optional, Union

from odd_models.models import (
    BinaryFieldStat,
    BooleanFieldStat,
    DataSetFieldStat,
    DateTimeFieldStat,
    IntegerFieldStat,
    NumberFieldStat,
    StringFieldStat,
)

from ..logger import logger
from ..models.column_statistics import ColumnStatistics


def _digit_checker(var, func, default):
    return func(var) if var is not None else default


def to_int(var) -> int:
    return _digit_checker(var, int, 0)


def to_float(var) -> float:
    return _digit_checker(var, float, 0.0)


ODDStatistics = Union[
    IntegerFieldStat,
    BinaryFieldStat,
    BooleanFieldStat,
    DateTimeFieldStat,
    NumberFieldStat,
    StringFieldStat,
]


def boolean_stats(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        boolean_stats=BooleanFieldStat(
            true_count=to_int(stats.num_trues),
            false_count=to_int(stats.num_trues),
            nulls_count=to_int(stats.num_trues),
        )
    )


def integer_stat(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        integer_stats=IntegerFieldStat(
            low_value=to_int(stats.min),
            high_value=to_int(stats.max),
            nulls_count=to_int(stats.num_nulls),
            unique_count=to_int(stats.distinct_count),
        )
    )


def float_stats(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        number_stats=NumberFieldStat(
            low_value=to_float(stats.min),
            high_value=to_float(stats.max),
            nulls_count=to_int(stats.num_nulls),
            unique_count=to_int(stats.distinct_count),
        )
    )


def string_stats(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        string_stats=StringFieldStat(
            max_length=to_int(stats.max_col_len),
            avg_length=to_float(stats.avg_col_len),
            nulls_count=to_int(stats.num_nulls),
            unique_count=to_int(stats.distinct_count),
        )
    )


def binary_stats(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        binary_stats=BinaryFieldStat(
            max_length=to_int(stats.max_col_len),
            avg_length=to_float(stats.avg_col_len),
            nulls_count=to_int(stats.num_nulls),
            unique_count=0,
        )
    )


def timestamp_stats(stats: ColumnStatistics) -> DataSetFieldStat:
    return DataSetFieldStat(
        datetime_stats=DateTimeFieldStat(
            low_value=datetime.fromtimestamp(to_int(stats.min)).astimezone(),
            high_value=datetime.fromtimestamp(to_int(stats.min)).astimezone(),
            nulls_count=to_int(stats.num_nulls),
            unique_count=to_int(stats.distinct_count),
        )
    )


HIVE_TYPE_STATISTICS: dict[str, Callable[[ColumnStatistics], DataSetFieldStat]] = {
    "boolean": boolean_stats,
    "tinyint": integer_stat,
    "smallint": integer_stat,
    "int": integer_stat,
    "bigint": integer_stat,
    "float": float_stats,
    "double": float_stats,
    "string": string_stats,
    "varchar": string_stats,
    "char": string_stats,
    "timestamp": timestamp_stats,
    "binary": binary_stats,
}


def map_statistic(statistics: ColumnStatistics) -> Optional[DataSetFieldStat]:
    try:
        fn = HIVE_TYPE_STATISTICS[statistics.data_type]
        return fn(statistics)
    except Exception as e:
        logger.warning(
            f"Error mapping statistic for column {statistics.column_name}: {e}"
        )
        logger.debug(traceback.format_exc())
        return None
