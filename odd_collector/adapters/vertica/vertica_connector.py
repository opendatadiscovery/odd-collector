from typing import List

import vertica_python

from .logger import logger
from .exceptions import DbVerticaSQLException


class VerticaConnector:
    __connection = None
    __cursor = None

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
            logger.error("Error during retrieving data from Database", exc_info=True)
            raise DbVerticaSQLException(
                "Database error. Troubles with connecting"
            ) from err
