from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin as BasePlugin
from typing_extensions import Annotated


class HostPortPlugin(BasePlugin):
    host: str
    port: int
    database: str
    user: str
    password: str


#  TODO: clear inheritance
class PostgreSQLPlugin(HostPortPlugin):
    type: Literal["postgresql"]


class MySQLPlugin(HostPortPlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(HostPortPlugin):
    type: Literal["clickhouse"]


class RedshiftPlugin(HostPortPlugin):
    type: Literal["redshift"]


class MongoDBPlugin(HostPortPlugin):
    type: Literal["mongodb"]
    protocol: str


class SnowflakePlugin(HostPortPlugin):
    type: Literal["snowflake"]
    account: str
    warehouse: str


class HivePlugin(HostPortPlugin):
    type: Literal["hive"]


class ElasticsearchPlugin(HostPortPlugin):
    type: Literal["elasticsearch"]
    http_auth: str = None
    use_ssl: bool = None
    verify_certs: bool = None
    ca_certs: str = None


class FeastPlugin(HostPortPlugin):
    type: Literal["feast"]
    repo_path: str


class CassandraPlugin(HostPortPlugin):
    type: Literal["cassandra"]
    contact_points: list = []


class KubeflowPlugin(BasePlugin):
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
