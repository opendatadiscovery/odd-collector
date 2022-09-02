from typing import List

import vertica_python
from odd_collector_sdk.errors import DataSourceError

from .logger import logger


class VerticaConnector:
    def __init__(self, config) -> None:
        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password
        self.database = config.database

    def execute(self, query: str) -> List[tuple]:
        try:
            with vertica_python.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwrod=self.password,
                database=self.database,
            ) as conn:
                with conn.cursor() as curs:
                    records = curs.execute(query).fetchall()
                    return records
        except vertica_python.Error as err:
            logger.error("Error during retrieving data from Database")
            raise DataSourceError("Database error. Troubles with connecting") from err
