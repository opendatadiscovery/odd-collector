from typing import Literal, Optional, Union

import pydantic
from odd_collector_sdk.domain.plugin import Plugin
from typing_extensions import Annotated


class BasePlugin(Plugin):
    host: str
    port: int
    database: str
    user: str
    password: str


class PostgreSQLPlugin(BasePlugin):
    type: Literal["postgresql"]


class MySQLPlugin(BasePlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class ClickhousePlugin(BasePlugin):
    type: Literal["clickhouse"]


class RedshiftPlugin(BasePlugin):
    type: Literal["redshift"]


class MongoDBPlugin(BasePlugin):
    type: Literal["mongodb"]
    protocol: str

class SnowflakePlugin(BasePlugin):
    type: Literal["snowflake"]
    account: str
    warehouse: str

class KafkaPlugin(Plugin):
    type: Literal["kafka"]
    schema_registry_conf: Optional[dict] = {}
    broker_conf: dict

AvailablePlugin = Annotated[
    Union[
        PostgreSQLPlugin,
        MySQLPlugin,
        ClickhousePlugin,
        RedshiftPlugin,
        MongoDBPlugin,
        SnowflakePlugin,
        KafkaPlugin
    ],
    pydantic.Field(discriminator="type"),
]
