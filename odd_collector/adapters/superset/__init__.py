_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/superset.json#/definitions/Superset"
)

_keys_to_include_dashboard = [
    "changed_on_delta_humanized",
    "changed_on_utc",
    "created_by",
    "created_on_delta_humanized",
    "status",
]

_keys_to_include_dataset = [
    "changed_on_delta_humanized",
    "changed_on_utc",
    "status",
    "changed_by_name",
    "datasource_type",
    "sql",
]
