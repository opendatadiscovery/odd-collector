from typing import List, Dict, Tuple, Any

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import (
    DataEntityList,
    DataEntity,
    DataEntityType,
    DataEntityGroup,
)


from oddrn_generator import VerticaGenerator
from .vertica_repository import VerticaRepository
from .mapper.tables import map_table
from .domain.table import Table
from .domain.column import Column


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__database = config.database
        self.__repository = VerticaRepository(config)
        self.__oddrn_generator = VerticaGenerator(
            host_settings=config.host, databases=config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        columns = self._get_columns()
        tables = self._get_tables()

        data_entities = []
        for table in tables:
            table.columns = columns.get((table.table_schema, table.table_name), [])
            data_entity = map_table(self.__oddrn_generator, table)
            data_entities.append(data_entity)

        db_entity = DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("databases"),
            name=self.__database,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            data_entity_group=DataEntityGroup(
                entities_list=[de.oddrn for de in data_entities]
            ),
        )
        data_entities.append(db_entity)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=data_entities
        )

    def _get_tables(self) -> List[Table]:
        tables = self.__repository.get_tables()
        return tables

    def _get_columns(self) -> Dict[Tuple[Any, Any], List[Column]]:
        columns = self.__repository.get_columns()
        column_dict = {}

        # create dict columns where key is unique combination of schema and table
        for column in columns:
            column_dict_key = (column.table_schema, column.table_name)
            if column_dict_key not in column_dict.keys():
                column_dict[column_dict_key] = []
            column_dict[column_dict_key].append(column)
        return column_dict
