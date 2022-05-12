from typing import Any, Dict, List, Set
from odd_models.models import MetadataExtension


def not_in_excluded(key, exclude_set):
    return key not in exclude_set


def not_empty(value):
    return value is not None and value != ""


def filter_metadata(data, excluded):
    return {
        k: v
        for k, v in data.items()
        if all([not_in_excluded(k, excluded), not_empty(v)])
    }


def extract_metadata(
    schema_url: str, entity: Dict[str, Any], excluded_key: Set = None
) -> List[MetadataExtension]:
    if not entity:
        return []

    if not excluded_key:
        excluded_key = set()

    metadata = filter_metadata(entity, excluded_key)

    return [MetadataExtension(schema_url=schema_url, metadata=metadata)]
