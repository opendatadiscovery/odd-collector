import contextlib
import logging

import tarantool
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import TarantoolGenerator

from odd_collector.domain.plugin import TarantoolPlugin

from .mappers.spaces import map_table


class Adapter(AbstractAdapter):
    __connection = None

    def __init__(self, config: TarantoolPlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__user = config.user
        self.__password = config.password
        self.__oddrn_generator = TarantoolGenerator(host_settings=self.__host)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def get_data_entities(self) -> list[DataEntity]:
        try:
            self.__connect()
            spaces = self.__space_select()
            row_numb = self.__rows_number_select(spaces)
            return map_table(self.__oddrn_generator, spaces, row_numb)
        except Exception as e:
            logging.error("Failed to load metadata for spaces")
            logging.exception(e)
        finally:
            self.__disconnect()
        return []

    def __connect(self):
        try:
            self.__connection = tarantool.connect(
                host=self.__host,
                port=self.__port,
                user=self.__user,
                password=self.__password,
            )
        except tarantool.DatabaseError as err:
            logging.error(err)
            logging.info(self.__host, self.__port, self.__user, self.__password)
            raise DBException("Database error") from err
        return

    def __space_select(self) -> list:
        # returns spaces metadata
        return self.__execute("box.space._space:select")[0]

    def __rows_number_select(self, spaces: list) -> dict[str:int]:
        # generates a dict of space_names and their number of rows
        space_names = [str(space[2]) for space in spaces]
        space_rows = {}
        for space_name in space_names:
            rows = int(str(self.__execute(f"box.space.{space_name}:count"))[1:])
            space_rows[space_name] = rows
        return space_rows

    def __disconnect(self):
        with contextlib.suppress(Exception):
            if self.__connection:
                self.__connection.close()
        return

    def __execute(self, command: str, *args):
        return self.__connection.call(command, args)


class DBException(Exception):
    pass
