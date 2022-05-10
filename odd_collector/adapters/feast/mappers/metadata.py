from typing import Any, Dict, Set

from humps import decamelize
from odd_models.models import MetadataExtension

SCHEMA_FILE_URL =  \
    'https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/' \
    'extensions/feast.json#/definitions/Feast'


class MetadataExtractor:
    __dataset_excludes = {'spec::name', 'spec::features', 'config::name'}

    def extract_dataset_metadata(self, data: Dict[str, Any]) -> MetadataExtension:
        return MetadataExtension(
            schema_url=SCHEMA_FILE_URL,
            metadata=self.__extract_all_entries(data, exclude=self.__dataset_excludes)
        )

    def __extract_all_entries(self,
                              dikt: Dict[str, Any],
                              exclude: Set[str],
                              prefix: str = None) -> Dict[str, Any]:
        entries = {}
        for k, v in dikt.items():
            key_prefix = k if prefix is None else f'{prefix}::{k}'

            if key_prefix not in exclude:
                if isinstance(v, dict):
                    entries.update(self.__extract_all_entries(v, exclude, key_prefix))
                else:
                    entries[decamelize(key_prefix)] = v
        return entries
