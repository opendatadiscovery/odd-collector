from odd_models.models import MetadataExtension


def _append_metadata_extension(metadata_list: list[MetadataExtension],
                               schema_url: str, metadata_dict: dict, excluded_keys: set = None):
    if not metadata_dict:
        return None
    metadata_wo_none = {
        key: value
        for key, value in metadata_dict.items()
        if key not in excluded_keys and value is not None and value != ''
    }
    metadata_list.append(MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none))
