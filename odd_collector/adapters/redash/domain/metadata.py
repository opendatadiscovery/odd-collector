from typing import Any, Dict, List

from odd_models.models import MetadataExtension


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
