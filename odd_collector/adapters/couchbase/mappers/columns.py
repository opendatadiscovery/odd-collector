from typing import List, Dict, T

from odd_models.models import DataSetField, DataSetFieldType, Type

from oddrn_generator import Generator
from odd_collector.adapters.couchbase.mappers.types import TYPES_COUCHBASE_TO_ODD


def map_columns(
    data: Dict[str, Dict[str, T]],
    oddrn_generator: Generator,
    parent_oddrn: str = None,
    columns: List[DataSetField] = None,
) -> List[DataSetField]:
    """Takes row query result and map columns recursively."""
    columns = columns if columns else []
    # ('activity', {'#docs': 3523, '%docs': 100, 'samples': ['buy', 'do', 'drink', 'eat', 'listing', 'see'], 'type': 'string'})
    for column_name, column_meta in data.items():
        # example of document content:
        # {'#docs': [916, 1], '%docs': [99.89, 0.1], 'samples': [[None], ['Les Rouges Gorges']], 'type': ['null', 'string']}
        data_type = column_meta["type"]
        if isinstance(data_type, list):
            types = [t for t in data_type if t != "null"]
            data_type = "union" if len(types) > 1 else types[0]
        if data_type == "number" and column_meta.get("samples"):
            # usually we will have list of samples {'samples': ['Les Rouges Gorges']}
            sample = column_meta["samples"][0]
            # some docs have values of different types {'samples': [[None], ['Les Rouges Gorges']]}
            if isinstance(sample, list):
                sample = [s for s in column_meta["samples"] if s[0] is not None][0]
            data_type = type(sample).__name__
        column = map_column(column_name, data_type, oddrn_generator, parent_oddrn)
        columns.append(column)
        if data_type == "object":
            map_columns(
                column_meta["properties"], oddrn_generator, column.oddrn, columns
            )
        if data_type == "array":
            map_columns(
                {"Values": column_meta["items"]}, oddrn_generator, column.oddrn, columns
            )
    return columns


def map_column(
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
