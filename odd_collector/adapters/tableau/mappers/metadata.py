from typing import Any, Dict, List, Set

from funcy import omit, select_values
from odd_models.models import MetadataExtension

from odd_collector.adapters.tableau.logger import logger


def not_empty(value):
    return value is not None and value != ""


def filter_metadata(entity: Dict[str, Any], excluded_key: Set[str]) -> Dict[str, Any]:
    return select_values(not_empty, omit(entity, excluded_key))


def extract_metadata(
    schema_url: str, metadata: Dict[str, Any], excluded_key: Set = None
) -> List[MetadataExtension]:
    try:
        if not metadata:
            return []

        if not excluded_key:
            excluded_key = set()

        metadata = filter_metadata(metadata, excluded_key)
        return [MetadataExtension(schema_url=schema_url, metadata=metadata)]
    except Exception as e:
        logger.exception(e)
        return []
