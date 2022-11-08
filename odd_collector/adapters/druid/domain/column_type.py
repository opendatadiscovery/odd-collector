from enum import Enum


class ColumnType(Enum):
    timestamp = "timestamp"
    dimension = "dimension"
    metric = "metric"
