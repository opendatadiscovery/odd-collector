from typing import List, Literal, Optional

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
    password: Optional[SecretStr]


class MySQLPlugin(DatabasePlugin):
    type: Literal["mysql"]
    ssl_disabled: Optional[bool] = False


class MSSQLPlugin(DatabasePlugin):
    type: Literal["mssql"]
    password: SecretStr
    port: str = 1433


class ClickhousePlugin(DatabasePlugin):
    type: Literal["clickhouse"]
    port: Optional[int]


class RedshiftPlugin(DatabasePlugin):
    type: Literal["redshift"]
    schemas: Optional[List[str]] = None
    password: SecretStr
    connection_timeout: Optional[int] = 10


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
    port: Optional[str] = None
    password: SecretStr
    account: Optional[str]
    warehouse: str  # active warehouse


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


class RedashPlugin(BasePlugin):
    type: Literal["redash"]
    server: str
    api_key: str


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


class MetabasePlugin(WithHost, WithPort):
    type: Literal["metabase"]
    login: str
    password: SecretStr


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


class OraclePlugin(WithHost, WithPort):
    user: str
    service: str
    type: Literal["oracle"]
    password: SecretStr


class MlflowPlugin(BasePlugin):
    type: Literal["mlflow"]
    dev_mode: bool = False
    tracking_uri: str
    registry_uri: str
    filter_experiments: Optional[
        List[str]
    ] = None  # List of pipeline names to filter, if omit fetch all pipelines


class AirbytePlugin(WithHost, WithPort):
    type: Literal["airbyte"]
    user: Optional[str]
    password: Optional[str]
    platform_host_url: str
    store_raw_tables: bool = True


class SingleStorePlugin(DatabasePlugin):
    type: Literal["singlestore"]
    ssl_disabled: Optional[bool] = False


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
    "redash": RedashPlugin,
    "odd_adapter": OddAdapterPlugin,
    "metabase": MetabasePlugin,
    "oracle": OraclePlugin,
    "mlflow": MlflowPlugin,
    "airbyte": AirbytePlugin,
    "singlestore": SingleStorePlugin,
}
