from odd_collector.domain.plugin import TableauPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import TableauGenerator

from .client import TableauClient
from .logger import logger
from .mappers.sheets import map_sheet

class Adapter(AbstractAdapter):
    def __init__(self, config: TableauPlugin) -> None:
        self.client = TableauClient(config)

        self.__oddrn_generator = TableauGenerator(
            host_settings=self.client.get_server_host(), sites=config.site
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        sheets = self.client.get_sheets()

        res = {}

        luids = set()
        for sheet in sheets:
            for field in sheet['datasourceFields']:
                for table in field['upstreamTables']:
                    luids.add(table['database']['luid'])

        databases = {dbs['luid']:dbs for dbs in self.client.get_databases_by_luid(luids)}
        
        for sheet in sheets:
            for field in sheet['datasourceFields']:
                for table in field['upstreamTables']:
                    database_luid = table['database']['luid']
                    database = databases[database_luid]

                    if database_luid not in res:
                        res[database_luid] = {
                            'table_name': table['name'],
                            'schema': table['schema'],
                            'database_name': database['name'],
                            'database_connection_type': database['connection_type'],
                            'database_host_name': database['host_name']
                        }

        sheets = map_sheet(self.__oddrn_generator, sheets, res)

        return DataEntityList(data_source_oddrn=self.get_data_source_oddrn(), items=sheets)