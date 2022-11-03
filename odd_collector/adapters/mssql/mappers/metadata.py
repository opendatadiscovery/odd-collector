from typing import List, Union

from odd_models.models import MetadataExtension

from odd_collector.adapters.mssql.models import Column, Table, View

from .helpers import __convert_bytes_to_str_in_dict

prefix = "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/extensions/mssql.json#/definitions"
dataset_url = f"{prefix}/MssqlDataSetExtension"
dataset_field_url = f"{prefix}/MssqlDataSetFieldExtension"


def map_metadata(entity: Union[View, Table, Column]) -> List[MetadataExtension]:
    scheme_url = dataset_field_url if entity is View else dataset_url
    return [
        MetadataExtension(
            schema_url=scheme_url,
            metadata=__convert_bytes_to_str_in_dict(entity.metadata),
        )
    ]
