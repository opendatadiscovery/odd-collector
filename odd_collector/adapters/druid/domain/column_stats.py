from dataclasses import dataclass

from odd_collector_sdk.errors import MappingDataError

from odd_collector.adapters.druid.domain.column_type import ColumnType


@dataclass
class ColumnStats:
    name: str
    type_signature: str
    data_type: str
    has_multiple_values: bool
    has_nulls: bool
    size: int
    cardinality: int
    min_value: any
    max_value: any
    error_message: any
    type: ColumnType = ColumnType.dimension

    @staticmethod
    def from_response(column_name: str, record: dict):
        try:
            # Return
            return ColumnStats(type_signature=record.get('typeSignature', None),
                               data_type=record.get('type', None),
                               has_multiple_values=record.get('hasMultipleValues', None),
                               has_nulls=record.get('hasNulls', None),
                               size=record.get('size', None),
                               cardinality=record.get('cardinality', None),
                               min_value=record.get('minValue', None),
                               max_value=record.get('maxValue', None),
                               error_message=record.get('errorMessage', None),
                               name=column_name)
        except Exception as e:
            # Throw
            raise MappingDataError("Couldn't transform Druid result to Column model") from e
