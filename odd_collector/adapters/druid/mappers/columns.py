from odd_models import models
from odd_models.models import DataSetField, DataSetFieldType

from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.types import TYPES_DRUID_TO_ODD


def column_to_data_set_field(oddrn_generator: DruidGenerator, column: Column) -> DataSetField:
    # Return
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", column.name),
        name=column.name,
        type=DataSetFieldType(
            type=TYPES_DRUID_TO_ODD.get(column.type, models.Type.TYPE_UNKNOWN),
            logical_type=column.type,
            is_nullable=column.is_nullable
        )
    )
