from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated


class Plugins(Plugin):
    host: str
    port: int
    database: str
    user: str
    password: str
    http_auth: str = None
    use_ssl: bool = None
    verify_certs: bool = None
    ca_certs: str = None


class PostgreSQLPlugin(Plugins):
    type: Literal["postgresql"]


class MySQLPlugin(Plugins):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(Plugins):
    type: Literal["clickhouse"]


class RedshiftPlugin(Plugins):
    type: Literal["redshift"]


class MongoDBPlugin(Plugins):
    type: Literal["mongodb"]
    protocol: str


class SnowflakePlugin(Plugins):
    type: Literal["snowflake"]
    account: str
    warehouse: str

      
class HivePlugin(Plugins):
    type: Literal["hive"]


class ElasticsearchPlugin(Plugins):
    type: Literal["elasticsearch"]


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
    ],
    pydantic.Field(discriminator="type"),
]
