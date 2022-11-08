_METADATA_SCHEMA_URL_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/redash.json#/definitions/Redash"
)

_keys_to_include_dashboard = ["tags", "updated_at", "created_at"]

_keys_to_include_query = [
    "retrieved_at",
    "updated_at",
    "query",
    "description",
    "created_at",
]
