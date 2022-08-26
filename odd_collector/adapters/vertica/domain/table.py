from typing import Any, List, Optional
from .column import Column


class Table:
    def __init__(
        self,
        table_schema: str,
        table_name: str,
        table_type: str,
        description: str,
        table_id: int,
        owner_name: str,
        create_time: str,
        table_rows: int,
        is_temp_table: bool,
        is_system_table: bool,
        view_definition: str,
        is_system_view: bool,
        columns: List[Column] = None
    ):
        self.table_schema = table_schema
        self.table_name = table_name
        self.table_type = table_type
        self.description = description or None
        self.table_id = table_id
        self.owner_name = owner_name
        self.create_time = create_time
        self.table_rows = table_rows
        self.is_temp_table = is_temp_table
        self.is_system_table = is_system_table
        self.view_definition = view_definition or None
        self.is_system_view = is_system_view
        self.columns = columns or []

    def get_oddrn(self, oddrn_generator):
        oddrn_path = "tables"
        if self.table_type == "VIEW":
            oddrn_path = "views"
        oddrn_generator.set_oddrn_paths(
            **{
                "schemas": self.table_schema,
                oddrn_path: self.table_name,
            }
        )

        return oddrn_generator.get_oddrn_by_path(oddrn_path)
