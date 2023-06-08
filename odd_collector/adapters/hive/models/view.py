import traceback
from dataclasses import dataclass
from typing import Optional

from sql_metadata.parser import Parser

from odd_collector.adapters.hive.logger import logger

from .table import Table


@dataclass
class View(Table):
    @property
    def depends_on(self) -> list[str]:
        if (
            not self.description
            or not self.description.view_definition
            or not self.description.view_definition.original_text
        ):
            return []

        return get_view_depended_names(self.description.view_definition.original_text)


def get_view_depended_names(view_text: Optional[str]) -> list[str]:
    if view_text is None:
        return []
    try:
        return Parser(view_text).tables
    except Exception as e:
        logger.warning(f"Could get table names from view. {e}")
        logger.debug(f"View {view_text}. {traceback.format_exc()}")
        return []
