from datetime import datetime
from typing import Optional, NamedTuple
from odd_collector.domain.utils import AnotherSqlParser

import pytz


def transform_datetime(table_time: datetime) -> Optional[str]:
    if table_time:
        table_time.astimezone(pytz.utc).isoformat()
    return None


class DbSchemaEntity(NamedTuple):
    db: str
    schema: str
    entity: str


class QueryEntityExtractor:
    def __init__(self, query: str):
        self.patched_query = AnotherSqlParser.patch_query(query).upper()

    def extract_table_name(self) -> str:
        first_split = self.patched_query.split("COPY INTO ")[1]
        second_split = first_split.split(" ")[0]
        return second_split.replace(" ", "")

    def extract_stage_name(self) -> str:
        first_split = self.patched_query.split("@")[1]
        return first_split.replace(" ", "").replace("(", "").replace(")", "")

    @staticmethod
    def extract_databases(entity_name: str) -> DbSchemaEntity:
        splitted = entity_name.split(".")
        return DbSchemaEntity(db=splitted[0], schema=splitted[1], entity=splitted[2])
