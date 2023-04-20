from typing import List

from odd_models.models import DataSetField, DataSetFieldType, Type

from oddrn_generator import Generator
from odd_collector.adapters.couchbase.mappers.types import TYPES_COUCHBASE_TO_ODD


def map_columns(data, oddrn_generator: Generator) -> List[DataSetField]:
    collector = []
    __map_columns(collector, data, oddrn_generator)
    return collector


def __map_columns(
    collector: list, data: dict, oddrn_generator: Generator, parent_oddrn: str = None
) -> List[DataSetField]:
    for key, value in data.items():
        # example of document content:
        # {'#docs': [916, 1], '%docs': [99.89, 0.1], 'samples': [[None], ['Les Rouges Gorges']], 'type': ['null', 'string']}
        data_type = value["type"]
        if isinstance(data_type, list):
            types = [t for t in data_type if t != "null"]
            data_type = "union" if len(types) > 1 else types[0]
        if data_type == "number" and value.get("samples"):
            # usually we will have list of samples {'samples': ['Les Rouges Gorges']}
            sample = value["samples"][0]
            # some docs have values of different types {'samples': [[None], ['Les Rouges Gorges']]}
            if isinstance(sample, list):
                sample = [s for s in value["samples"] if s[0] is not None][0]
            data_type = type(sample).__name__
        column = __map_column(key, data_type, oddrn_generator, parent_oddrn)
        collector.append(column)
        if data_type == "object":
            __map_columns(collector, value["properties"], oddrn_generator, column.oddrn)
        if data_type == "array":
            __map_columns(
                collector, {"Values": value["items"]}, oddrn_generator, column.oddrn
            )


def __map_column(
    name: str, data_type: str, oddrn_generator: Generator, parent_oddrn: str = None
) -> DataSetField:
    oddrn = (
        f"{parent_oddrn}/subcolumns/{name}"
        if parent_oddrn
        else oddrn_generator.get_oddrn_by_path("columns", name)
    )
    return DataSetField(
        oddrn=oddrn,
        name=name,
        parent_field_oddrn=parent_oddrn,
        metadata=[],
        type=DataSetFieldType(
            type=TYPES_COUCHBASE_TO_ODD.get(data_type, Type.TYPE_UNKNOWN),
            logical_type=data_type,
            is_nullable="null" in data_type,
        ),
    )
