from typing import Type
from urllib.parse import urlparse

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import Generator, TableauGenerator

from odd_collector.adapters.tableau.domain.table import EmbeddedTable
from odd_collector.domain.plugin import TableauPlugin

from .client import TableauClient
from .logger import logger
from .mappers.sheets import map_sheet
from .mappers.tables import map_table


class Adapter(BaseAdapter):
    config: TableauPlugin
    generator: TableauGenerator

    def __init__(
        self, config: TableauPlugin, client: Type[TableauClient] = TableauClient
    ) -> None:
        super().__init__(config)
        self.client = client(config)

    def create_generator(self) -> Generator:
        site = self.config.site or "default"
        host = urlparse(self.config.server).netloc
        return TableauGenerator(host_settings=host, sites=site)

    def get_data_entity_list(self) -> DataEntityList:
        sheets = self.client.get_sheets()
        tables = self.client.get_tables()

        embedded_tables: list[EmbeddedTable] = [
            t for t in tables.values() if isinstance(t, EmbeddedTable)
        ]

        tbl_entities = {
            table.id: map_table(self.generator, table) for table in embedded_tables
        }

        sheets_data_entities = []
        for sheet in sheets:
            sheet_entity = map_sheet(self.generator, sheet)

            for table_id in sheet.tables_id:
                table = tables.get(table_id)

                if not table:
                    logger.warning(f"Table {table_id} not found in tables, skipping it")
                    continue

                if table.is_embedded:
                    oddrn = tbl_entities[table_id].oddrn
                else:
                    oddrn = tables.get(table_id).get_oddrn()

                sheet_entity.data_consumer.inputs.append(oddrn)

            sheets_data_entities.append(sheet_entity)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tbl_entities.values(), *sheets_data_entities],
        )
