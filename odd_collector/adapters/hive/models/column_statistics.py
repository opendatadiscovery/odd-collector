from dataclasses import dataclass


@dataclass
class ColumnStatistics:
    column_name: str
    data_type: str
    min: str
    max: str
    num_nulls: str
    distinct_count: str
    avg_col_len: str
    max_col_len: str
    num_trues: str
    num_falses: str
    comment: str
