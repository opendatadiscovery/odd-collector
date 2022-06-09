from typing import Dict, Any

from odd_models.models import DataEntityType, DataEntity, DataSet

from . import metadata_extractor
from .fields import map_field


def map_index(index_meta: Dict[str, Any], mapping, oddrn_generator) -> DataEntity:

    oddrn_generator.set_oddrn_paths(indexes=index_meta["index"])
    index_oddrn = oddrn_generator.get_oddrn_by_path("indexes")

    # field type with `@` prefix defines alias for another field in same index
    fields = [
        map_field(fn, mapping[fn], oddrn_generator)
        for fn in mapping
        if not fn.startswith("@")
    ]

    return DataEntity(
        oddrn=index_oddrn,
        name=index_meta["index"],
        owner=None,
        description=index_meta["index"],
        type=DataEntityType.TABLE,
        metadata=[metadata_extractor.extract_index_metadata(index_meta)],
        dataset=DataSet(parent_oddrn=None, rows_number=0, field_list=list(fields)),
    )
