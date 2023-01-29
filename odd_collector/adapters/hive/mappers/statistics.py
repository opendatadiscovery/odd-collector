from odd_models.models import (
    BinaryFieldStat,
    BooleanFieldStat,
    DateTimeFieldStat,
    NumberFieldStat,
    StringFieldStat,
)
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
    ColumnStatisticsData,
)

DEFAULT_VALUE = -1


def _mapper_numeric(stats_data: ColumnStatisticsData):
    columns_stat_type = (
            stats_data.longStats or stats_data.doubleStats or stats_data.dateStats
    )
    return {
        "low_value": _digit_checker(columns_stat_type.lowValue, float),
        "high_value": _digit_checker(columns_stat_type.highValue, float),
        "mean_value": None,
        "median_value": None,
        "nulls_count": _digit_checker(columns_stat_type.numNulls, int),
        "unique_count": _digit_checker(columns_stat_type.numDVs, int),
    }


def _mapper_decimal(stats_data: ColumnStatisticsData):
    return {
        "low_value": _digit_checker(stats_data.decimalStats.lowValue, int),
        "high_value": _digit_checker(stats_data.decimalStats.highValue, int),
        "mean_value": None,
        "median_value": None,
        "nulls_count": _digit_checker(stats_data.decimalStats.numNulls, int),
        "unique_count": _digit_checker(stats_data.decimalStats.numDVs, int),
    }


def _mapper_bytes(stats_data: ColumnStatisticsData):
    columns_stat_type = stats_data.stringStats or stats_data.binaryStats
    return {
        "max_length": _digit_checker(columns_stat_type.maxColLen, int),
        "avg_length": _digit_checker(columns_stat_type.avgColLen, float),
        "nulls_count": _digit_checker(columns_stat_type.numNulls, int),
        "unique_count": _digit_checker(columns_stat_type.numDVs, int),
    }


def _mapper_boolean(stats_data: ColumnStatisticsData):
    return {
        "true_count": _digit_checker(stats_data.booleanStats.numTrues, int),
        "false_count": _digit_checker(stats_data.booleanStats.numFalses, int),
        "nulls_count": _digit_checker(stats_data.booleanStats.numNulls, int),
    }


def _digit_checker(var, func):
    return func(var) if var is not None else DEFAULT_VALUE


FIELD_TYPE_SCHEMA = {
    "boolean": {
        "odd_type": BooleanFieldStat,
        "field_name": "boolean_stats",
        "mapper": _mapper_boolean,
    },
    "date": {
        "odd_type": DateTimeFieldStat,
        "field_name": "date_time_stats",
        "mapper": _mapper_numeric,
    },
    "timestamp": {
        "odd_type": DateTimeFieldStat,
        "field_name": "date_time_stats",
        "mapper": _mapper_numeric,
    },
    "decimal": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_decimal,
    },
    "tinyint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "smallint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "int": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "bigint": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "float": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "double": {
        "odd_type": NumberFieldStat,
        "field_name": "number_stats",
        "mapper": _mapper_numeric,
    },
    "string": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "varchar": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "char": {
        "odd_type": StringFieldStat,
        "field_name": "string_stats",
        "mapper": _mapper_bytes,
    },
    "binary": {
        "odd_type": BinaryFieldStat,
        "field_name": "binary_stats",
        "mapper": _mapper_bytes,
    },
}
