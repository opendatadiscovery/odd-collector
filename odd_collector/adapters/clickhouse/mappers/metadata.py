from collections import namedtuple
from odd_models.models import MetadataExtension

from typing import List


def _append_metadata_extension(metadata_list: List[MetadataExtension],
                               schema_url: str, named_tuple: namedtuple, excluded_keys: set = None):
    if named_tuple is not None and len(named_tuple) > 0:
        metadata: dict = named_tuple._asdict()
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None:
                metadata_wo_none[key] = value
        metadata_list.append(MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none))
