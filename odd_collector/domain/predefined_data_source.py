"""
PredefinedDataSource class used for BI services when DataEntities use external resources
For an example: Cube's cube uses postgres table, we have no information about that data source to generate valid oddrn for input
"""
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import pydantic
from odd_models.utils import SqlParser
from oddrn_generator.generators import ClickHouseGenerator, PostgresqlGenerator


class PredefinedDatasourceParams(pydantic.BaseModel):
    type: str
    host: Optional[str] = None
    database: Optional[str] = None


class PredefinedDataSource(ABC):
    def __init__(self, params: PredefinedDatasourceParams):
        self._params = params
        self._generator = None

    @abstractmethod
    def get_inputs_oddrn(self, sql: str) -> List[str]:
        """
        Args:
            sql: sql statements string
        Examples:
            "SELECT name FROM entity"
        Returns:
            List of input oddrns, i.e ['//postgres/host/localhost/database/account/schema/public/entity']
        """
        raise NotImplementedError


class PostgresDatasource(PredefinedDataSource):
    def __init__(self, params):
        super().__init__(params)
        self.generator = PostgresqlGenerator(host_settings=self._params.host)
        self.generator.set_oddrn_paths(databases=self._params.database)

    def get_inputs_oddrn(self, sql: str) -> List[str]:
        inputs, _ = SqlParser(sql=sql.strip("`")).get_response()
        return [self.generate_oddrn_for_table(table_name) for table_name in inputs]

    def generate_oddrn_for_table(self, input: str):
        schema, table = input.split(".")
        self.generator.set_oddrn_paths(schemas=schema, tables=table)
        return self.generator.get_oddrn_by_path("tables")


class ClickHouseDatasource(PredefinedDataSource):
    def __init__(self, params):
        super().__init__(params)
        self.generator = ClickHouseGenerator(
            host_settings=self._params.host, databases=self._params.database
        )

    def get_inputs_oddrn(self, sql: str) -> List[str]:
        inputs, _ = SqlParser(sql=sql.strip("`")).get_response()
        return [self.generate_oddrn_for_table(table_name) for table_name in inputs]

    def generate_oddrn_for_table(self, input: str):
        database, table = input.split(".")
        self.generator.set_oddrn_paths(databases=database, tables=table)
        return self.generator.get_oddrn_by_path("tables")


DATA_SOURCES = {"postgres": PostgresDatasource, "clickhouse": ClickHouseDatasource}


def create_predefined_datasource(
    params: PredefinedDatasourceParams,
) -> PredefinedDataSource:
    try:
        return DATA_SOURCES[params.type](params)
    except KeyError:
        logging.error(f"There are no PredefinedDataSource for {params.type}")
        raise
