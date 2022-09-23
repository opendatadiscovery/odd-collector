from typing import Optional

from oddrn_generator.path_models import BasePathsModel


class DruidPathsModel(BasePathsModel):
    catalogs: Optional[str]
    schemas: Optional[str]
    tables: Optional[str]
    columns: Optional[str]

    class Config:
        dependencies_map = {
            "catalogs": ("catalogs",),
            "schemas": ("catalogs", "schemas"),
            "tables": ("catalogs", "schemas", "tables"),
            "columns": ("catalogs", "schemas", "tables", "columns"),
        }
