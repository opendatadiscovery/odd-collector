from odd_models.models import MetadataExtension
from typing import List, Dict, Any, Union
from .dashboard import Dashboard
from .dataset import Dataset


def create_metadata_extension_list(
    schema_url: str, metadata: Dict[str, Any], keys_to_include: List[str]
) -> List[MetadataExtension]:
    return [
        MetadataExtension(
            schema_url=schema_url,
            metadata={
                key: value for key, value in metadata.items() if key in keys_to_include
            },
        )
    ]


def add_owner(
    metanode: Dict[Any, Any], data_unit: Union[Dashboard, Dataset]
) -> Union[Dashboard, Dataset]:
    owners = metanode.get("owners")
    if (owners is not None) and (len(owners) > 0):
        owner = owners[0].get("username")
        if owner is not None:
            data_unit.owner = owner
    return data_unit
