import logging
from oddrn_generator import SnowflakeGenerator
from snowflake import connector
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from .mappers import _tables_select, _columns_select
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config):
        self.__warehouse = config.warehouse
        self.__database = config.database
        self.__username = config.user
        self.__password = config.password
        self.__account = config.account
        self.__oddrn_generator = SnowflakeGenerator(
            host_settings=f"{self.__account}.snowflakecomputing.com",
            warehouses=self.__warehouse,
            databases=self.__database,
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(_tables_select)
            columns = self.__execute(_columns_select)

            return map_table(self.__oddrn_generator, tables, columns)
        except Exception as e:
            logging.error('Failed to load metadata for tables')
            logging.exception(e)
        finally:
            self.__disconnect()
        return []

    def __execute(self, query: str) -> list[tuple]:
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def __connect(self):
        try:
            self.__connection = connector.connect(
                user=self.__username,
                password=self.__password,
                account=self.__account
            )
            self.__cursor = self.__connection.cursor().execute(f"USE DATABASE {self.__database}")
        except Exception as err:
            logging.error(err)
            raise DBException('Database error')
        return

    def __disconnect(self):
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception:
            pass
        try:
            if self.__connection:
                self.__connection.close()
        except Exception:
            pass
        return


class DBException(Exception):
    pass
