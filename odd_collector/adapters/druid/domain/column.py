from dataclasses import dataclass

from odd_collector_sdk.errors import MappingDataError


@dataclass
class Column:
    catalog: str
    schema: str
    table: str
    name: str
    type: str
    is_nullable: bool
    ordinal_position: int

    @staticmethod
    def from_response(record: dict):
        try:
            # Return
            return Column(
                record["TABLE_CATALOG"],
                record["TABLE_SCHEMA"],
                record["TABLE_NAME"],
                record["COLUMN_NAME"],
                record["DATA_TYPE"],
                True if record["IS_NULLABLE"] == "YES" else False,
                record["ORDINAL_POSITION"],
            )
        except Exception as e:
            # Throw
            raise MappingDataError(
                "Couldn't transform Druid result to Column model"
            ) from e
