from typing import Dict, List, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import TableauGenerator

from odd_collector.domain.plugin import TableauPlugin

from .client import TableauBaseClient, TableauClient
from .domain.table import EmbeddedTable, Table
from .mappers.sheets import map_sheet
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    def __init__(
        self, config: TableauPlugin, client: Type[TableauBaseClient] = None
    ) -> None:
        client = client or TableauClient
        self.client = client(config)

        self.__oddrn_generator = TableauGenerator(
            host_settings=self.client.get_server_host(), sites=config.site
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        sheets = self._get_sheets()
        tables = self._get_tables()

        tables_data_entities_by_id: Dict[str, DataEntity] = {
            table_id: map_table(self.__oddrn_generator, table)
            for table_id, table in tables.items()
        }
        tables_data_entities = tables_data_entities_by_id.values()

        sheets_data_entities = []
        for sheet in sheets:
            sheet_tables = [
                tables_data_entities_by_id[table_id] for table_id in sheet.tables_id
            ]
            data_entity = map_sheet(self.__oddrn_generator, sheet, sheet_tables)
            sheets_data_entities.append(data_entity)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_data_entities, *sheets_data_entities],
        )

    def _get_tables(self) -> Dict[str, Table]:
        tables: List[Table] = self.client.get_tables()
        tables_by_id: Dict[str, Table] = {table.id: table for table in tables}

        ids = tables_ids_to_load(tables)
        tables_columns = self.client.get_tables_columns(ids)

        for table_id, columns in tables_columns.items():
            tables_by_id[table_id].columns = columns

        return tables_by_id

    def _get_sheets(self):
        return self.client.get_sheets()


def tables_ids_to_load(tables: List[Table]):
    return [table.id for table in tables if isinstance(table, EmbeddedTable)]
