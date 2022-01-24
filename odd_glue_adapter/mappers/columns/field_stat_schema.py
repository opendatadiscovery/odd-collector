import base64
from typing import Any, Dict

from odd_models.models import (BooleanFieldStat, DateTimeFieldStat, NumberFieldStat,
                               StringFieldStat, BinaryFieldStat)


def _mapper_numeric(glue_data: Dict[str, Any]):
    return {
        'low_value': glue_data['MinimumValue'],
        'high_value': glue_data['MaximumValue'],
        'mean_value': None,
        'median_value': None,
        'nulls_count': glue_data['NumberOfNulls'],
        'unique_count': glue_data['NumberOfDistinctValues']
    }


def _mapper_decimal(glue_data: Dict[str, Any]):
    minimum_value = int(base64.b64decode(glue_data['MinimumValue']['UnscaledValue'])) / \
                    int(glue_data['MinimumValue']['Scale'])
    maximum_value = int(base64.b64decode(glue_data['MinimumValue']['UnscaledValue'])) / \
                    int(glue_data['MaximumValue']['Scale'])

    return {
        'low_value': minimum_value,
        'high_value': maximum_value,
        'mean_value': None,
        'median_value': None,
        'nulls_count': glue_data['NumberOfNulls'],
        'unique_count': glue_data['NumberOfDistinctValues']
    }


def _mapper_bytes(glue_data: Dict[str, Any]):
    return {
        'max_length': glue_data['MaximumLength'],
        'avg_length': glue_data['AverageLength'],
        'nulls_count': glue_data['NumberOfNulls'],
        'unique_count': glue_data.get('NumberOfDistinctValues', None)
    }


def _mapper_boolean(glue_data: Dict[str, Any]):
    return {
        'true_count': glue_data['NumberOfTrues'],
        'false_count': glue_data['NumberOfFalses'],
        'nulls_count': glue_data['NumberOfNulls']
    }


FIELD_TYPE_SCHEMA = {
    'BOOLEAN': {
        'odd_type': BooleanFieldStat,
        'glue_type_name': 'BooleanColumnStatisticsData',
        'field_name': 'boolean_stats',
        'mapper': _mapper_boolean
    },
    'DATE': {
        'odd_type': DateTimeFieldStat,
        'glue_type_name': 'DateColumnStatisticsData',
        'field_name': 'date_time_stats',
        'mapper': _mapper_numeric
    },
    'DECIMAL': {
        'odd_type': NumberFieldStat,
        'glue_type_name': 'DecimalColumnStatisticsData',
        'field_name': 'number_stats',
        'mapper': _mapper_decimal
    },
    'DOUBLE': {
        'odd_type': NumberFieldStat,
        'glue_type_name': 'DoubleColumnStatisticsData',
        'field_name': 'number_stats',
        'mapper': _mapper_numeric
    },
    'LONG': {
        'odd_type': NumberFieldStat,
        'glue_type_name': 'LongColumnStatisticsData',
        'field_name': 'number_stats',
        'mapper': _mapper_numeric
    },
    'STRING': {
        'odd_type': StringFieldStat,
        'glue_type_name': 'StringColumnStatisticsData',
        'field_name': 'string_stats',
        'mapper': _mapper_bytes
    },
    'BINARY': {
        'odd_type': BinaryFieldStat,
        'glue_type_name': 'BinaryColumnStatisticsData',
        'field_name': 'binary_stats',
        'mapper': _mapper_bytes
    }
}
