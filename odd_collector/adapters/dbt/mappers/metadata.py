from odd_models.models import MetadataExtension


def _append_metadata_extension(metadata_list: list[MetadataExtension],
                               schema_url: str, metadata_dict: dict, excluded_keys: set = None):
    if metadata_dict is not None and len(metadata_dict) > 0:
        metadata: dict = metadata_dict.copy()
        if excluded_keys is not None:
            for key in excluded_keys:
                metadata.pop(key)
        metadata_wo_none: dict = {}
        for key, value in metadata.items():
            if value is not None and value != '':
                metadata_wo_none[key] = value
        metadata_list.append(MetadataExtension(schema_url=schema_url, metadata=metadata_wo_none))
