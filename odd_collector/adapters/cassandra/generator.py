from typing import Optional

from oddrn_generator import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel
from pydantic import Field


class CassandraPathsModel(BasePathsModel):
    keyspaces: str
    tables: Optional[str]
    views: Optional[str]
    columns: Optional[str]
    tables_columns: Optional[str] = Field(alias="columns")
    views_columns: Optional[str] = Field(alias="columns")

    class Config:
        dependencies_map = {
            "keyspaces": ("keyspaces",),
            "tables": ("keyspaces", "tables"),
            "views": ("keyspaces", "views"),
            "tables_columns": ("keyspaces", "tables", "tables_columns"),
            "views_columns": ("keyspaces", "views", "views_columns"),
        }
        data_source_path = "keyspaces"


class CassandraGenerator(Generator):
    source = "cassandra"
    paths_model = CassandraPathsModel
    server_model = HostnameModel
