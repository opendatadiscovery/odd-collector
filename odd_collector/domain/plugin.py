from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin as BasePlugin
from typing_extensions import Annotated


class WithHost(BasePlugin):
    host: str


class WithPort(BasePlugin):
    port: str


class DatabasePlugin(WithHost, WithPort):
    database: Optional[str]
    user: str
    password: str


class PostgreSQLPlugin(DatabasePlugin):
    type: Literal["postgresql"]


class MySQLPlugin(DatabasePlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class MSSQLPlugin(DatabasePlugin):
    type: Literal["mssql"]
    driver: str


class ClickhousePlugin(DatabasePlugin):
    type: Literal["clickhouse"]


class RedshiftPlugin(DatabasePlugin):
    type: Literal["redshift"]


class MongoDBPlugin(DatabasePlugin):
    type: Literal["mongodb"]
    protocol: str

class KafkaPlugin(BasePlugin):
    type: Literal["kafka"]
    host: str
    port: int
    schema_registry_conf: Optional[dict] = {}
    broker_conf: dict

class SnowflakePlugin(DatabasePlugin):
    type: Literal["snowflake"]
    account: str
    warehouse: str


class HivePlugin(WithHost, WithPort):
    type: Literal["hive"]


class ElasticsearchPlugin(WithHost, WithPort):
    type: Literal["elasticsearch"]
    http_auth: str = None
    use_ssl: bool = None
    verify_certs: bool = None
    ca_certs: str = None


class FeastPlugin(WithHost):
    type: Literal["feast"]
    repo_path: str


class DbtPlugin(WithHost):
    type: Literal["dbt"]
    odd_catalog_url: str


class CassandraPlugin(DatabasePlugin):
    type: Literal["cassandra"]
    contact_points: list = []


class KubeflowPlugin(BasePlugin):
    type: Literal["kubeflow"]
    host: str
    namespace: str
    session_cookie0: Optional[str]
    session_cookie1: Optional[str]


class TarantoolPlugin(DatabasePlugin):
    type: Literal["tarantool"]


class Neo4jPlugin(DatabasePlugin):
    type: Literal["neo4j"]


class TableauPlugin(BasePlugin):
    type: Literal["tableau"]
    server: str
    site: str
    user: str
    password: str


AvailablePlugin = Annotated[
    Union[
        PostgreSQLPlugin,
        MySQLPlugin,
        MSSQLPlugin,
        ClickhousePlugin,
        RedshiftPlugin,
        MongoDBPlugin,
        KafkaPlugin,
        SnowflakePlugin,
        HivePlugin,
        ElasticsearchPlugin,
        FeastPlugin,
        DbtPlugin,
        CassandraPlugin,
        KubeflowPlugin,
        TarantoolPlugin,
        Neo4jPlugin,
        TableauPlugin
    ],
    pydantic.Field(discriminator="type"),
]
