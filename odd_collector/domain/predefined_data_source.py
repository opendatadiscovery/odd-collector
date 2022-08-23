"""
PredefinedDataSource class used for BI services when DataEntities use external resources
For an example: Cube's cube uses postgres table, we have no information about that data source to generate valid oddrn for input
"""
from abc import ABC, abstractmethod
from typing import Optional

import pydantic
from oddrn_generator.generators import PostgresqlGenerator


class PredefinedDatasourceParams(pydantic.BaseModel):
    type: str
    host: Optional[str] = None
    database: Optional[str] = None


class PredefinedDataSource(ABC):
    def __init__(self, params: PredefinedDatasourceParams):
        self._params = params
        self._generator = None

    @abstractmethod
    def get_oddrn_for(self, **new_path) -> str:
        """
        Args:
            **new_path: new path for Generator:
        Examples:
            pg_generator.get_oddrn_for({"schemas": "public", "tables": "pc"})
        """
        raise NotImplementedError


class PostgresDatasource(PredefinedDataSource):
    def __init__(self, params):
        super().__init__(params)
        self.generator = PostgresqlGenerator(host_settings=self._params.host)
        self.generator.set_oddrn_paths(databases=self._params.database)

    def get_oddrn_for(self, **new_path) -> str:
        self.generator.set_oddrn_paths(**new_path)
        return self.generator.get_oddrn_by_path("tables")


DATA_SOURCES = {"postgres": PostgresDatasource}


def create_predefined_datasource(
    params: PredefinedDatasourceParams,
) -> PredefinedDataSource:
    return DATA_SOURCES[params.type](params)
