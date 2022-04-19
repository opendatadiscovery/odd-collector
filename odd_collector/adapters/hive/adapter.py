import logging
from time import perf_counter
from hive_metastore_client import HiveMetastoreClient
from typing import List, Dict
from more_itertools import flatten

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import HiveGenerator
from .mappers.tables import map_hive_table
from .mappers.columns.main import map_column_stats


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__oddrn_generator = HiveGenerator(host_settings=f"{self.__host}", databases=config.database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        data_entities = self.get_data_entities()
        logging.info(f"Hive adapter data entities: {data_entities}")
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=data_entities,
        )

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()
            logging.info(f"Hive adapter connected to {self.__host}:{self.__port}")
        except Exception as e:
            logging.warning(f"Hive adapter connect failed: {e}")
            return []
        try:
            start = perf_counter()
            database_list = self.__cursor.get_all_databases()
            tables_list = list(flatten(self.__get_tables(db) for db in database_list))
            self.__disconnect()
            logging.info(
                f"Hive adapter loaded {len(tables_list)} DataEntity(s) from database"
                f" in {perf_counter() - start} seconds")
            return tables_list
        except Exception as e:
            logging.warning(f"Hive adapter no datasets found: {e}")
        return []

    def __get_tables(self, db: str) -> List[DataEntity]:
        tables_list = self.__cursor.get_all_tables(db)
        if tables_list:
            aggregated_table_stats = [self.__cursor.get_table(db, table) for table in tables_list]
            output = [self.__process_table_raw_data(table_stats) for table_stats in aggregated_table_stats]
            return output
        else:
            return []

    def __process_table_raw_data(self, table_stats) -> DataEntity:
        columns = {c.name: c.type for c in table_stats.sd.cols}
        unmapped_stats = self.__get_columns_stats(table_stats, columns)
        stats = map_column_stats(unmapped_stats) or None
        result = map_hive_table(self.__host, table_stats, columns, stats)
        return result

    def __get_columns_stats(self, table_stats, columns: Dict) -> List:
        if table_stats.parameters.get('COLUMN_STATS_ACCURATE', None):
            stats = []
            for column_name in columns.keys():
                try:
                    result = self.__cursor.get_table_column_statistics(table_stats.dbName,
                                                                       table_stats.tableName,
                                                                       column_name)
                except Exception as e:
                    logging.warning(f"Hive adapter can't retrieve statistics for '{column_name}' in '{table_stats.tableName}'. "
                                    f"Generated an exception: {e}. "
                                    f"May be the type: array, struct, map, union")
                else:
                    stats.append(result)
            return stats
        else:
            logging.info(f"Hive adapter table statistics for '{table_stats.tableName}' is not available. "
                         f"Stats has not been gathered. ")
            return []

    def __connect(self):
        self.__connection = HiveMetastoreClient(self.__host, self.__port)
        self.__cursor = self.__connection.open()

    def __disconnect(self):
        try:
            if self.__cursor:
                self.__cursor.close()
                logging.info("Hive adapter connection is closed")
        except Exception as e:
            logging.warning(f"Hive adapter disconnect failed: {e}")
