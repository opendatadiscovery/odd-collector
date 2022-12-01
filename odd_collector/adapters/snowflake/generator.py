from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class SnowflakePathsModel(BasePathsModel):
    databases: str
    schemas: Optional[str]
    tables: Optional[str]
    views: Optional[str]
    tables_columns: Optional[str] = Field(alias="columns")
    views_columns: Optional[str] = Field(alias="columns")

    class Config:
        dependencies_map = {
            "databases": ("databases",),
            "schemas": ("databases", "schemas"),
            "tables": ("databases", "schemas", "tables"),
            "views": ("databases", "schemas", "views"),
            "tables_columns": ("databases", "schemas", "tables", "tables_columns"),
            "views_columns": ("databases", "schemas", "views", "views_columns"),
        }
        data_source_path = "databases"


class SnowflakeGenerator(Generator):
    source = "snowflake"
    paths_model = SnowflakePathsModel
    server_model = HostnameModel
