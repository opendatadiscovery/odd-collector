from typing import NamedTuple, Optional

from odd_models.models import MetadataExtension


def append_metadata_extension(
    metadata_list: list[MetadataExtension],
    schema_url: str,
    named_tuple: NamedTuple,
    excluded_keys: set = None,
):
    if named_tuple is not None and len(named_tuple) > 0:
        metadata: dict = named_tuple._asdict()
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None and convert_bytes_to_str(value) != "":
                metadata_wo_none[key] = convert_bytes_to_str(value)
        metadata_list.append(
            MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none)
        )


def convert_bytes_to_str(value: Optional[bytes]) -> Optional[str]:
    return value if type(value) is not bytes else value.decode("utf-8")
