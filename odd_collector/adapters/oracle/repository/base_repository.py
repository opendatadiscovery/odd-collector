from typing import Any, Protocol


class Repository(Protocol):
    def get_schemas(self) -> Any:
        ...

    def get_databases(self) -> Any:
        ...

    def get_tables(self) -> Any:
        ...

    def get_columns(self) -> Any:
        ...
