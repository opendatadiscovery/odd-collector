from odd_models.models import MetadataExtension

from odd_collector.adapters.hive.models.base_table import BaseTable

SCHEMA_FILE_URL = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/"
    "main/specification/extensions/hive.json"
)


def map_metadata(table: BaseTable) -> list[MetadataExtension]:
    return [
        MetadataExtension(
            schema_url=f"{SCHEMA_FILE_URL}#/definitions/HiveDataSetExtension",
            metadata=table.metadata,
        )
    ]
