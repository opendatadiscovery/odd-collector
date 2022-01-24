from typing import Dict, Any

from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType, DataSet, DataSetField
from oddrn_generator import GlueGenerator

from . import metadata_extractor
from .columns import map_column


def map_glue_table(raw_table_data: Dict[str, Any],
                   column_stats: Dict[str, DataSetField],
                   oddrn_generator: GlueGenerator) -> DataEntity:
    oddrn_generator.set_oddrn_paths(databases=raw_table_data['DatabaseName'], tables=raw_table_data['Name'])
    table_oddrn = oddrn_generator.get_oddrn_by_path('tables')

    owner_name = raw_table_data.get('Owner', None)
    owner_oddrn = oddrn_generator.get_oddrn_by_path('owners', owner_name) if owner_name is not None else None

    columns = flatten([
        map_column(rc, oddrn_generator, column_stats.get(rc['Name'], None))
        for rc in raw_table_data['StorageDescriptor']['Columns']
    ])

    return DataEntity(
        oddrn=table_oddrn,
        name=raw_table_data['Name'],
        description=raw_table_data.get('Description', None),
        type=DataEntityType.TABLE,
        owner=owner_oddrn,
        updated_at=raw_table_data['UpdateTime'].isoformat(),
        created_at=raw_table_data['CreateTime'].isoformat(),
        metadata=[metadata_extractor.extract_dataset_metadata(raw_table_data)],
        dataset=DataSet(
            parent_oddrn=None,
            rows_number=0,
            field_list=list(columns)
        )
    )
