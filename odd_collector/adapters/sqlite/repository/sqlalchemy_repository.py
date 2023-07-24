from typing import Any, Dict, Iterable, List

import sqlalchemy as db
from funcy import lmap
from sqlalchemy.util import FacadeDict

from odd_collector.domain.plugin import SQLitePlugin

from ..domain import Column, Table, View
from .base_repository import Repository


def create_column(data: Dict[str, Any]) -> Column:
    return Column(
        name=data.get("name"),
        type=data["type"],
        primary_key=bool(data.get("primary_key")),
        nullable=data.get("nullable"),
        default=data.get("default"),
        logical_type=data.get("type"),
        autoincrement=data.get("autoincrement"),
    )


class SqlAlchemyRepository(Repository):
    def __init__(self, config: SQLitePlugin) -> None:
        self._config = config
        self._eng = self._create_engine()
        self._meta = db.MetaData(bind=self._eng)
        self._meta.reflect(views=True)
        self._tables: db.util.FacadeDict[str, db.Table] = self._meta.tables
        self._inspector = db.inspect(self._eng)

    def get_tables(self) -> Iterable[Table]:
        names = self._inspector.get_table_names()
        for table_name in names:
            table = Table(
                name=table_name,
                columns=self._get_columns(table_name),
            )
            yield table

    def get_views(self) -> Iterable[View]:
        names = self._inspector.get_view_names()
        for view_name in names:
            view = View(
                name=view_name,
                columns=self._get_columns(view_name),
                view_definition=self._inspector.get_view_definition(view_name),
            )
            yield view

    def _get_columns(self, table_name: str) -> List[Column]:
        return lmap(create_column, self._inspector.get_columns(table_name))

    def _create_engine(self) -> db.engine.Engine:
        connection_str = f"sqlite:///{self._config.data_source}"
        return db.create_engine(connection_str)
