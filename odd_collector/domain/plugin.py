from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin as BasePlugin
from typing_extensions import Annotated


class WithHost(BasePlugin):
    host: str


class WithPort(BasePlugin):
    port: str


class DatabasePlugin(WithHost, WithPort):
    database: str
    user: str
    password: str


class PostgreSQLPlugin(DatabasePlugin):
    type: Literal["postgresql"]


class MySQLPlugin(DatabasePlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(DatabasePlugin):
    type: Literal["clickhouse"]


class RedshiftPlugin(DatabasePlugin):
    type: Literal["redshift"]


class MongoDBPlugin(DatabasePlugin):
    type: Literal["mongodb"]
    protocol: str


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


class CassandraPlugin(DatabasePlugin):
    type: Literal["cassandra"]
    contact_points: list = []


class KubeflowPlugin(BasePlugin):
    type: Literal["kubeflow"]
    host: str
    namespace: str
    session_cookie0: Optional[str]
    session_cookie1: Optional[str]


AvailablePlugin = Annotated[
    Union[
        PostgreSQLPlugin,
        MySQLPlugin,
        ClickhousePlugin,
        RedshiftPlugin,
        MongoDBPlugin,
        SnowflakePlugin,
        HivePlugin,
        ElasticsearchPlugin,
        FeastPlugin,
        CassandraPlugin,
        KubeflowPlugin,
    ],
    pydantic.Field(discriminator="type"),
]
