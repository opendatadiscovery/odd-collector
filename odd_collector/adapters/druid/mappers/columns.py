from typing import List, Optional

from odd_models import models
from odd_models.models import DataSetField, DataSetFieldType, Tag, DataSetFieldStat, StringFieldStat, NumberFieldStat

from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.domain.column_stats import ColumnStats
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.types import TYPES_DRUID_TO_ODD


def column_to_data_set_field(oddrn_generator: DruidGenerator, column: Column, column_stats: ColumnStats) -> DataSetField:
    tags: List[Tag] = []
    logical_type = column.type

    # Check
    if column_stats is not None:
        # Add tags
        tags.append(Tag(name=f"max_value:{column_stats.max_value}"))
        tags.append(Tag(name=f"min_value:{column_stats.min_value}"))
        tags.append(Tag(name=f"has_multiple_values:{column_stats.has_multiple_values}"))
        tags.append(Tag(name=f"size:{column_stats.size / 1024} mb"))
        tags.append(Tag(name=f"type:{column_stats.type}"))

        # Get logical type
        logical_type = column_stats.data_type

    # Return
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", column.name),
        name=column.name,
        tags=tags,
        stats=column_stats_to_data_set_field_stat(column_stats),
        type=DataSetFieldType(
            type=TYPES_DRUID_TO_ODD.get(column.type, models.Type.TYPE_UNKNOWN),
            logical_type=logical_type,
            is_nullable=column.is_nullable
        )
    )


def column_stats_to_data_set_field_stat(column_stats: ColumnStats) -> Optional[DataSetFieldStat]:
    if column_stats is None:
        return None
    elif column_stats.data_type.upper() == 'STRING':
        # Char, Varchar
        return DataSetFieldStat(
            string_stats=StringFieldStat(
                max_length=0,
                avg_length=0,
                nulls_count=0,
                unique_count=column_stats.cardinality or 0
            )
        )
    elif column_stats.data_type.upper() == 'LONG':
        # Boolean, Tiny int, Small int, Integer, Big integer, Timestamp, Date
        return DataSetFieldStat(
            number_stats=NumberFieldStat(
                low_value=column_stats.min_value or 0,
                high_value=column_stats.max_value or 0,
                nulls_count=0,
                unique_count=column_stats.cardinality or 0
            )
        )
    elif column_stats.data_type.upper() == 'FLOAT' or column_stats == 'DOUBLE':
        # Decimal, Float, Real, Double
        return None
    else:
        # Complex type
        return None