from typing import Set

TABLEAU_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_SCHEMA_PREFIX: str = (
    "https://raw.githubusercontent.com/opendatadiscovery/opendatadiscovery-specification/main/specification/"
    "extensions/tableau.json#/definitions/Tableau"
)

DATA_SET_SCHEMA: str = f"{_SCHEMA_PREFIX}DataSetExtension"
DATA_SET_FIELD_SCHEMA: str = f"{_SCHEMA_PREFIX}DataSetFieldExtension"
DATA_CONSUMER_SCHEMA: str = f"{_SCHEMA_PREFIX}DataConsumerExtension"

DATA_SET_EXCLUDED_KEYS: Set[str] = {
    "name",
    "schema",
    "description",
    "contact",
    "certifier",
    "database",
    "columns",
}
DATA_SET_FIELD_EXCLUDED_KEYS: Set[str] = {"name", "description", "remoteType"}
DATA_CONSUMER_EXCLUDED_KEYS: Set[str] = {
    "name",
    "createdAt",
    "updatedAt",
    "workbook",
    "datasourceFields",
}
