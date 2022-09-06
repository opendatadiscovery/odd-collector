from dataclasses import asdict
from typing import Union

from funcy import omit, select_values
from odd_models.models import MetadataExtension

from ..domain import Column, Table
from ..logger import logger


def extract_metadata(schema_url: str, excluded_keys: set, entity: Union[Column, Table]):
    try:
        if excluded_keys is None:
            excluded_keys = set()

        metadata = omit(asdict(entity), excluded_keys)
        wo_none = select_values(lambda v: v is not None, metadata)

        return MetadataExtension(schema_url=schema_url, metadata=wo_none)
    except Exception:
        logger.debug("Couldn't extract metadata", exc_info=True)
        return None
