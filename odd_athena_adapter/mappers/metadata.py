import json
from typing import Any, Dict, Set, Callable

from humps.main import decamelize

SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/" \
                  "main/specification/extensions/athena.json"


class MetadataExtractor:
    __dataset_excludes = {'Name', 'CreateTime', 'TableType', 'Columns'}

    __preprocessing: Dict[str, Callable] = {
        "Parameters::exclusions": json.loads
    }

    def extract_dataset_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'schema_url': f'{SCHEMA_FILE_URL}#/definitions/AthenaDataSetExtension',
            'metadata': self.__extract_all_entries(data, exclude=self.__dataset_excludes)
        }

    def __extract_all_entries(self,
                              dikt: Dict[str, Any],
                              exclude: Set[str],
                              prefix: str = None) -> Dict[str, Any]:
        entries = {}
        for k, v in dikt.items():
            key_prefix = k if prefix is None else f'{prefix}::{k}'

            if key_prefix not in exclude:
                if key_prefix in self.__preprocessing:
                    v = self.__preprocessing[key_prefix](v)

                entries[decamelize(k)] = self.__extract_all_entries(v, exclude, key_prefix) \
                    if isinstance(v, dict) \
                    else v

        return entries
