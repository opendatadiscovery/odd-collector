from odd_models.models import MetadataExtension

SCHEMA_FILE_URL = ("https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
                   "main/specification/extensions/hive.json")


def _metadata(table_stats) -> MetadataExtension:
    return MetadataExtension(schema_url=f"{SCHEMA_FILE_URL}#/definitions/HiveDataSetExtension",
                             metadata=table_stats.parameters or {})
