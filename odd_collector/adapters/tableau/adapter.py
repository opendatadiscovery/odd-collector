import logging


from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from odd_collector.domain.plugin import TableauPlugin
from oddrn_generator import TableauGenerator


from .mappers.sheets import map_sheet
from .mappers.tables import map_table
from .client import TableauClient


class Adapter(AbstractAdapter):
    def __init__(self, config: TableauPlugin) -> None:
        self.client = TableauClient(config)

        self.__oddrn_generator = TableauGenerator(
            host_settings=self.client.get_server_host(), sites=config.site
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=self.__get_datasets()
        )

    def __get_datasets(self) -> list[DataEntity]:
        try:
            tables = self.client.get_tables()
            sheets = self.client.get_sheets()

            m_tables = map_table(self.__oddrn_generator, tables)
            m_sheets = map_sheet(self.__oddrn_generator, sheets)

            return [*m_tables, *m_sheets]
        except Exception as e:
            logging.exception("Error during gettin datasets")
            return []
