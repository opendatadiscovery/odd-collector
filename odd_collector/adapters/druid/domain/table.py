import dataclasses

from odd_collector_sdk.errors import MappingDataError


@dataclasses.dataclass
class Table:
    catalog: str
    schema: str
    name: str
    type: str
    is_joinable: bool
    is_broadcast: bool

    @staticmethod
    def from_response(record: dict):
        try:
            # Return
            return Table(
                record["TABLE_CATALOG"],
                record["TABLE_SCHEMA"],
                record["TABLE_NAME"],
                record["TABLE_TYPE"],
                True if record["IS_JOINABLE"] == "YES" else False,
                True if record["IS_BROADCAST"] == "YES" else False,
            )
        except Exception as e:
            # Throw
            raise MappingDataError(
                "Couldn't transform Druid result to Table model"
            ) from e
