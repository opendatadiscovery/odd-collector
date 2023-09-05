import dataclasses
import traceback
from typing import Any, Optional

from odd_collector_sdk.utils.metadata import HasMetadata
from sql_metadata import Parser

from odd_collector.helpers.datetime import Datetime

from ..logger import logger
from .column import Column


@dataclasses.dataclass
class Dependency:
    name: str
    schema: str

    @property
    def uid(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclasses.dataclass
class Table(HasMetadata):
    catalog: str
    schema: str
    name: str
    type: str
    create_time: Datetime
    update_time: Datetime
    table_rows: Optional[int] = None
    comment: Optional[str] = None
    sql_definition: Optional[str] = None
    columns: list["Column"] = dataclasses.field(default_factory=list)
    metadata: dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def odd_metadata(self):
        return self.metadata

    @property
    def uid(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def dependencies(self) -> list[Dependency]:
        try:
            sql = self.sql_definition

            if not sql:
                return []

            if isinstance(sql, bytes):
                sql = sql.decode("utf-8")

            parsed = Parser(sql.replace("(", "").replace(")", ""))
            dependencies = []

            for dependency in parsed.tables:
                schema_name = dependency.split(".")

                if len(schema_name) != 2:
                    logger.warning(
                        f"Dependency must be in format <schema>.<table>. got {dependency=}"
                    )
                    continue

                schema, name = schema_name
                dependencies.append(Dependency(name=name, schema=schema))
            return dependencies
        except Exception as e:
            logger.warning(f"Couldn't parse dependencies from {self.uid}. {e}")
            logger.debug(self.sql_definition)
            logger.debug(traceback.format_exc())
            return []
