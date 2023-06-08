from dataclasses import dataclass
from typing import Optional

from funcy import omit

from ..logger import logger
from .column import Column
from .table_description import TableDescription


@dataclass
class Table:
    name: str
    description: TableDescription

    @property
    def columns(self) -> list[Column]:
        return self.description.columns

    @property
    def primary_keys(self) -> list[str]:
        return self.description.primary_keys

    @property
    def rows_number(self) -> Optional[int]:
        return self.description.table_parameters.get("numRows")

    @property
    def odd_metadata(self) -> dict[str, str]:
        try:
            table_params = omit(
                self.description.table_parameters, ["COLUMN_STATS_ACCURATE"]
            )

            return {
                **self.description.storage_information,
                **self.description.storage_description_params,
                **table_params,
                **self.description.detailed_table_information,
            }
        except Exception as e:
            logger.warning(f"Could not get table metadata for {self.name}. {e}")
            return {}

    @property
    def owner(self) -> Optional[str]:
        return self.description.detailed_table_information.get("Owner")

    @property
    def create_time(self) -> Optional[str]:
        return self.description.detailed_table_information.get("CreateTime")
