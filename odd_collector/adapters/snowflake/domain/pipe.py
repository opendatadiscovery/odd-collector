from typing import NamedTuple, Optional

from pydantic import BaseModel

from odd_collector.domain.utils import AnotherSqlParser

from .entity import Connectable


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
        second_split = first_split.split(" ")[0]
        return second_split.replace(" ", "").replace("(", "").replace(")", "")

    @staticmethod
    def extract_databases(entity_with_dot: str) -> DbSchemaEntity:
        splitted = entity_with_dot.split(".")
        return DbSchemaEntity(db=splitted[0], schema=splitted[1], entity=splitted[2])


class RawPipe(BaseModel):
    pipe_catalog: str
    pipe_schema: str
    pipe_name: str
    definition: str

    @property
    def __qee(self):
        return QueryEntityExtractor(self.definition)

    @property
    def downstream(self) -> str:
        table_name = self.__qee.extract_table_name()
        if table_name.count(".") == 2:
            return table_name + ".TABLE"
        elif table_name.count(".") == 0:
            return f"{self.pipe_catalog.upper()}.{self.pipe_schema.upper()}.{table_name.upper()}.TABLE"
        else:
            raise NotImplementedError(f"Unusual table name {table_name}")

    @property
    def stage_full_name(self) -> str:
        stage_name = self.__qee.extract_stage_name()
        if stage_name.count(".") == 2:
            return stage_name
        elif stage_name.count(".") == 0:
            return f"{self.pipe_catalog.upper()}.{self.pipe_schema.upper()}.{stage_name.upper()}"
        else:
            raise NotImplementedError(f"Unusual stage name {stage_name}")


class RawStage(BaseModel):
    stage_name: str
    stage_catalog: str
    stage_schema: str
    stage_url: Optional[str]
    stage_type: str

    @property
    def stage_full_name(self) -> str:
        return f"{self.stage_catalog.upper()}.{self.stage_schema.upper()}.{self.stage_name.upper()}"


class Pipe(Connectable):
    name: str
    definition: str
    stage_url: Optional[str]
    stage_type: str
