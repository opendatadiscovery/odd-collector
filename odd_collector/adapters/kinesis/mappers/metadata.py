import json
from typing import Any, Dict, Set, Callable

SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/"


class MetadataExtractor:

    # attributes to exclude from metadata
    __data_entity_excludes = {'StreamName'}

    __preprocessing: Dict[str, Callable] = {
        "Parameters::exclusions": json.loads
    }

    def extract_data_entity_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        extract all metadata from the given data
        :param data: the data we want to extract from
        :return: dictionary containing schema url and metadata
        """
        return {
            'schema_url': SCHEMA_FILE_URL,
            'metadata': self.__extract_all_entries(data, exclude=self.__data_entity_excludes)
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

                entries[k] = self.__extract_all_entries(v, exclude) \
                    if isinstance(v, dict) \
                    else v

        return entries
