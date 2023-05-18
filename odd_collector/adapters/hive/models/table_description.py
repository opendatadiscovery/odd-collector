from typing import Optional, Type

from ..logger import logger
from .column import Column, parse_column_type
from .view_definition import ViewDefinition

Row = tuple[str, str, str]


class TableDescription:
    def __init__(self):
        self.columns: list[Column] = []
        self.detailed_table_information = {}
        self.storage_information = {}
        self.storage_description_params = {}
        self.table_parameters = {}
        self.foreign_keys = []
        self.primary_keys = []
        self.view_definition: Optional[ViewDefinition] = None


class State:
    """
    State machine for parsing table description.
    """

    def __init__(self, context: TableDescription):
        self.context = context

    def next(self, row: Row) -> "State":
        key, _, _ = row

        if self.empty_row(row):
            return self
        elif new_state := STATE_MAP.get(key.strip()):
            return new_state(self.context)
        else:
            return self._next(row)

    def _next(self, row: Row) -> "State":
        raise NotImplementedError

    def __repr__(self):
        return self.__class__.__name__

    @staticmethod
    def empty_row(row: Row) -> bool:
        return not any(row)


class Initial(State):
    """
    Initial state. We are looking for the first column name.
    """

    def _next(self, row: Row) -> "State":
        return self


class Columns(State):
    """
    We are in the columns section. We are looking for the next column name.
    """

    def _next(self, row: Row) -> "State":
        col_name, col_type, comment = row

        self.context.columns.append(
            Column(col_name, parse_column_type(col_type), comment)
        )
        return self


class DetailedTableInformation(State):
    """
    We are in the detailed table information section.
    """

    def _next(self, row: Row) -> "State":
        key, value, comment = row

        key = key.strip().strip(":")
        value = value.strip() if value else None

        self.context.detailed_table_information[key] = value
        return self


class TableParameters(State):
    def _next(self, row: Row) -> "State":
        _, key, value = row
        key = key.strip().strip(":")
        value = value.strip() if value else None

        self.context.table_parameters[key] = value
        return self


class StorageInformation(State):
    def _next(self, row: Row) -> "State":
        key, value, comment = row
        key = key.strip().strip(":")
        value = value.strip() if value else None

        self.context.storage_information[key] = value
        return self


class StorageDescriptionParams(State):
    def _next(self, row: Row) -> "State":
        _, key, value = row
        key = key.strip().strip(":")
        value = value.strip() if value else None
        self.context.storage_description_params[key] = value
        return self


class Constraints(State):
    def _next(self, row: Row) -> "State":
        key, value, comment = row
        logger.debug(f"Constraint: {key}, {value}, {comment}")
        return self


class ForeignKeys(State):
    def _next(self, row: Row) -> "State":
        key, value, comment = row
        logger.debug(f"Foreign key: {key}, {value}, {comment}")
        self.context.foreign_keys.append((key, value, comment))
        return self


class PrimaryKeys(State):
    def _next(self, row: Row) -> "State":
        key, value, comment = row
        logger.debug(f"Primary key: {key}, {value}, {comment}")
        self.context.primary_keys.append((key, value, comment))
        return self


class ViewInformation(State):
    def _next(self, row: Row) -> "State":
        key, value, comment = row
        if key.startswith("View Original Text:"):
            self.context.view_definition = ViewDefinition(
                original_text=value, expanded_text="", rewrite_enabled=""
            )
        elif key.startswith("View Expanded Text:"):
            self.context.view_definition.expanded_text = value
        elif key.startswith("View Rewrite Enabled:"):
            self.context.view_definition.rewrite_enabled = value

        logger.debug(f"View information: {key}, {value}, {comment}")
        return self


STATE_MAP: dict[str, Type[State]] = {
    "# col_name": Columns,
    "# Detailed Table Information": DetailedTableInformation,
    "Table Parameters:": TableParameters,
    "# Storage Information": StorageInformation,
    "Storage Desc Params:": StorageDescriptionParams,
    "# Constraints": Constraints,
    "# Foreign Keys": ForeignKeys,
    "# Primary Key": PrimaryKeys,
    "# View Information": ViewInformation,
}
