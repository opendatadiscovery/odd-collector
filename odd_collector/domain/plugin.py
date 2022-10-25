from typing import Literal, Optional

from odd_collector_sdk.domain.plugin import Plugin as BasePlugin
from odd_collector_sdk.types import PluginFactory
from pydantic import SecretStr, validator

from odd_collector.domain.predefined_data_source import PredefinedDatasourceParams


class WithPredefinedDataSource:
    predefined_datasource: PredefinedDatasourceParams


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


class OdbcPlugin(DatabasePlugin):
    type: Literal["odbc"]
    driver: str = "{ODBC Driver 17s for SQL Server}"


class MySQLPlugin(DatabasePlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class MSSQLPlugin(DatabasePlugin):
    type: Literal["mssql"]
    driver: str


class ClickhousePlugin(DatabasePlugin):
    type: Literal["clickhouse"]
    port: Optional[int]


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
    database: str


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
    user: Optional[str]
    password: Optional[SecretStr]
    token_name: Optional[str]
    token_value: Optional[SecretStr]
    pagination_size: int = 10


class DruidPlugin(BasePlugin):
    type: Literal["druid"]
    host: str
    port: int


class VerticaPlugin(DatabasePlugin):
    type: Literal["vertica"]


class SupersetPlugin(BasePlugin):
    type: Literal["superset"]
    server: str
    username: str
    password: str


class CubeJSPlugin(BasePlugin):
    type: Literal["cubejs"]
    host: str
    dev_mode: bool = False
    token: Optional[SecretStr]
    predefined_datasource: PredefinedDatasourceParams

    @validator("token")
    def validate_token(cls, value: Optional[SecretStr], values):
        if values.get("dev_mode") == False and value is None:
            raise ValueError("Token must be set in production mode")

        return value


class PrestoPlugin(BasePlugin):
    type: Literal["presto"]
    host: str
    port: int
    user: str
    principal_id: Optional[str]
    password: Optional[str]


class TrinoPlugin(BasePlugin):
    type: Literal["trino"]
    host: str
    port: int
    user: str
    password: Optional[str]


class OddAdapterPlugin(BasePlugin):
    type: Literal["odd_adapter"]
    host: str
    data_source_oddrn: str


PLUGIN_FACTORY: PluginFactory = {
    "postgresql": PostgreSQLPlugin,
    "mysql": MySQLPlugin,
    "mssql": MSSQLPlugin,
    "clickhouse": ClickhousePlugin,
    "redshift": RedshiftPlugin,
    "mongodb": MongoDBPlugin,
    "kafka": KafkaPlugin,
    "snowflake": SnowflakePlugin,
    "hive": HivePlugin,
    "elasticsearch": ElasticsearchPlugin,
    "feast": FeastPlugin,
    "dbt": DbtPlugin,
    "cassandra": CassandraPlugin,
    "kubeflow": KubeflowPlugin,
    "tarantool": TarantoolPlugin,
    "neo4j": Neo4jPlugin,
    "tableau": TableauPlugin,
    "cubejs": CubeJSPlugin,
    "odbc": OdbcPlugin,
    "presto": PrestoPlugin,
    "trino": TrinoPlugin,
    "vertica": VerticaPlugin,
    "druid": DruidPlugin,
    "superset": SupersetPlugin,
    "odd_adapter": OddAdapterPlugin
}
