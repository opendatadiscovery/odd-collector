from contextlib import contextmanager
import pyodbc
import logging
from .exception import DBException


@contextmanager
def logging_exception(exc_msg):
    """ Wrap any code block with this context manager to catch exceptions occurring in it and log them"""
    try:
        yield None
    except Exception:
        logging.exception(exc_msg)


@contextmanager
def connect_odbc(data_source: str):
    logging.debug(f"Connecting to data source: {data_source}")
    connection = None
    cursor = None
    try:
        connection = pyodbc.connect(data_source)
        cursor = connection.cursor()
        yield cursor
    except Exception as e:
        logging.error(e)
        raise DBException(f"Database error ({data_source})")
    finally:
        if cursor:
            with logging_exception("closing cursor"):
                cursor.close()
        if connection:
            with logging_exception("closing connection"):
                connection.close()
