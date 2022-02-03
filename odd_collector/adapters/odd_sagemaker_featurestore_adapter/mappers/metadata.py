from typing import Any, Dict, Set, Callable, List

from humps import decamelize
from odd_models.models import MetadataExtension

SCHEMA_FILE_URL = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/" \
                  "main/specification/extensions/sagemaker_featurestore.json"


class __MetadataExtractor:
    __dataset_excludes = {'FeatureGroupName', 'FeatureDefinitions', 'CreationTime'}

    __preprocessing: Dict[str, Callable] = {}

    def extract_dataset_metadata(self, data: Dict[str, Any]) -> List[MetadataExtension]:
        return [MetadataExtension(
            schema_url=f'{SCHEMA_FILE_URL}#/definitions/SagemakerFeaturestoreExtension',
            metadata=self.__extract_all_entries(data, exclude=self.__dataset_excludes)
        )]

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


metadata_extractor = __MetadataExtractor()
