from typing import Any, Dict, Set

from humps import decamelize


SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/" \
                  "main/specification/extensions/elasticsearch.json"


class MetadataExtractor:
    __index_excludes = {'uuid', 'index', 'pri', 'rep'}

    def extract_index_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'schema_url': f'{SCHEMA_FILE_URL}#/definitions/ElasticSearchDataSetExtension',
            'metadata': self.__extract_all_entries(data, exclude=self.__index_excludes)
        }

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
