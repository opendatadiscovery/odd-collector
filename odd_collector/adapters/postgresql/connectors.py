import contextlib

import psycopg2

from odd_collector.domain.plugin import PostgreSQLPlugin


@contextlib.contextmanager
def connect(config: PostgreSQLPlugin):
    params = {
        "dbname": config.database,
        "user": config.user,
        "password": config.password.get_secret_value(),
        "host": config.host,
        "port": config.port,
    }

    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    try:
        yield cur
    finally:
        cur.close()
        conn.close()
