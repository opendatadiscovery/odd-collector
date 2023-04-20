from typing import Optional

import oddrn_generator
from pydantic import BaseModel, Field, validator

from odd_collector.logger import logger


# TODO: update this module as fivetran constantly updates connectors/destinations options.
class DatabaseConfig(BaseModel):
    host_settings: str = Field(..., alias="host")
    databases: str = Field(..., alias="database")


class KafkaConfig(BaseModel):
    host_settings: str = Field(..., alias="servers")
    clusters: str = Field(..., alias="servers")


class OracleConfig(DatabaseConfig):
    schemas: str = Field(..., alias="user")


class MongoConfig(DatabaseConfig):
    host_settings: str = Field(..., alias="hosts")

    @validator("host_settings", pre=True, always=True)
    def set_host_settings(cls, v):
        return v[0]


class DatabricksConfig(BaseModel):
    pass


class KinesisConfig(BaseModel):
    pass


class S3Config(BaseModel):
    pass


class DatasetGenerator:
    GENERATORS = {
        "apache_kafka": (oddrn_generator.KafkaGenerator, DatabaseConfig),
        "postgres": (oddrn_generator.PostgresqlGenerator, DatabaseConfig),
        "postgres_warehouse": (oddrn_generator.PostgresqlGenerator, DatabaseConfig),
        "mysql": (oddrn_generator.MysqlGenerator, DatabaseConfig),
        "mysql_warehouse": (oddrn_generator.MysqlGenerator, DatabaseConfig),
        "databricks": (oddrn_generator.DatabricksLakehouseGenerator, DatabricksConfig),
        "oracle": (oddrn_generator.OracleGenerator, OracleConfig),
        "redshift": (oddrn_generator.RedshiftGenerator, DatabaseConfig),
        "snowflake_db": (oddrn_generator.SnowflakeGenerator, DatabaseConfig),
        "snowflake": (oddrn_generator.SnowflakeGenerator, DatabaseConfig),
        "kinesis": (oddrn_generator.KinesisGenerator, KinesisConfig),
        "mongo": (oddrn_generator.MongoGenerator, DatabaseConfig),
        "s3": (oddrn_generator.S3Generator, S3Config),
        "azure_sql_data_warehouse": (oddrn_generator.AzureSQLGenerator, DatabaseConfig),
    }

    @classmethod
    def get_generator(cls, name: str, **kwargs) -> Optional[oddrn_generator.Generator]:
        try:
            generator_cls, config_model = cls.GENERATORS[name]
            config = config_model(**kwargs).dict()
            return generator_cls(**config)
        except KeyError:
            logger.warning(f"Generator not available for dataset: {name}")
            return None
