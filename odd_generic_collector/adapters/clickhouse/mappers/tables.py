from typing import List

import pytz
from odd_models.models import DataEntity, DataSet, DataEntityType
from oddrn_generator import ClickHouseGenerator

from . import MetadataNamedtuple, ColumnMetadataNamedtuple, _data_set_metadata_schema_url, \
    _data_set_metadata_excluded_keys
from .columns import map_column
from .metadata import _append_metadata_extension
from .transformer import extract_transformer_data


def map_table(oddrn_generator: ClickHouseGenerator,
              tables: List[tuple], columns: List[tuple], integration_engines: List[tuple]) -> List[DataEntity]:
    data_entities: List[DataEntity] = []
    column_index: int = 0

    for table in tables:
        mtable: MetadataNamedtuple = MetadataNamedtuple(*table)
        table_name: str = mtable.name

        data_entity_type = DataEntityType.VIEW if mtable.engine in ["View", "MaterializedView"] else DataEntityType.TABLE
        oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

        # DataEntity
        data_entity: DataEntity = DataEntity(
            oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path, table_name),
            name=table_name,
            owner=None,
            description=None,
            metadata=[],
            type=data_entity_type,
        )
        data_entities.append(data_entity)

        _append_metadata_extension(data_entity.metadata, _data_set_metadata_schema_url, mtable,
                                   _data_set_metadata_excluded_keys)

        if "metadata_modification_time" in mtable and mtable.metadata_modification_time is not None:
            data_entity.updated_at = mtable.metadata_modification_time.replace(tzinfo=pytz.utc).isoformat()

        # Dataset
        data_entity.dataset = DataSet(
            rows_number=int(mtable.total_rows) if mtable.total_rows is not None else None,
            field_list=[]
        )

        # DataTransformer
        if data_entity.type == DataEntityType.VIEW:
            data_entity.data_transformer = extract_transformer_data(mtable, oddrn_generator, integration_engines)

        # DatasetField
        while column_index < len(columns):
            column: tuple = columns[column_index]
            mcolumn: ColumnMetadataNamedtuple = ColumnMetadataNamedtuple(*column)

            if mcolumn.table == table_name:
                data_entity.dataset.field_list.append(map_column(mcolumn, oddrn_generator, data_entity.owner, oddrn_path))
                column_index += 1
            else:
                break

    return data_entities
