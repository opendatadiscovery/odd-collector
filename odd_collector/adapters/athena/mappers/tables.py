from typing import Dict, Any

from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import AthenaGenerator

from . import metadata_extractor
from .columns import map_column
from .types import TABLE_TYPES_SQL_TO_ODD


def map_athena_table(raw_table_data: Dict[str, Any],
                     catalog_name: str,
                     database_name: str,
                     oddrn_generator: AthenaGenerator) -> DataEntity:

    data_entity_type = TABLE_TYPES_SQL_TO_ODD.get(raw_table_data['TableType'], DataEntityType.UNKNOWN)
    oddrn_path = "views" if data_entity_type == DataEntityType.VIEW else "tables"

    oddrn_generator.set_oddrn_paths(**{
        'catalogs': catalog_name, 'databases': database_name, oddrn_path: raw_table_data['Name']
    })
    columns = flatten([
        map_column(rc, oddrn_generator, oddrn_path)
        for rc in raw_table_data['Columns']
    ])

    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path(oddrn_path),
        name=raw_table_data['Name'],
        type=data_entity_type,
        created_at=raw_table_data['CreateTime'].isoformat(),
        metadata=[metadata_extractor.extract_dataset_metadata(raw_table_data)],
        dataset=DataSet(
            rows_number=0,
            field_list=list(columns)
        )
    )
