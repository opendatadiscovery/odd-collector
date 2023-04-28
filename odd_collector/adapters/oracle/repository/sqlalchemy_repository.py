import sys
from typing import Any, Dict, Iterable, List

import oracledb
import sqlalchemy as db
from funcy import lmap
from sqlalchemy.util import FacadeDict

from odd_collector.domain.plugin import OraclePlugin

from ..domain import Column, Dependency, DependencyType, Table, View
from .base_repository import Repository

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb


def create_column(data: Dict[str, Any]) -> Column:
    return Column(
        name=data.get("name"),
        type=data["type"],
        is_literal=data.get("is_literal"),
        primary_key=data.get("primary_key"),
        nullable=data.get("nullable"),
        default=data.get("default"),
        index=data.get("index"),
        unique=data.get("unique"),
        comment=data.get("comment"),
        logical_type=data.get("type"),
    )


class SqlAlchemyRepository(Repository):
    def __init__(self, config: OraclePlugin) -> None:
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
                description=self._get_comment(table_name),
            )
            yield table

    def get_views(self) -> Iterable[View]:
        names = self._inspector.get_view_names()
        dependencies = self._get_dependencies()

        views: Dict[str, View] = {
            view_name: View(
                name=view_name,
                columns=self._get_columns(view_name),
                view_definition=self._inspector.get_view_definition(view_name),
                description=self._get_comment(view_name),
                upstream=[],
                downstream=[],
            )
            for view_name in names
        }

        for dep in dependencies:
            if dep.name in views:
                views[dep.name].upstream.append(dep)

                if (
                    dep.referenced_type == DependencyType.VIEW
                    and dep.referenced_name in views
                ):
                    views[dep.referenced_name].downstream.append(dep)

        return iter(views.values())

    def _get_dependencies(self) -> Iterable[Dependency]:
        """Get dependencies for views"""
        with self._eng.connect() as connection:
            cur = connection.execute(
                """
                SELECT d.name, d.referenced_owner, d.referenced_name, d.referenced_type
                FROM user_dependencies d
                WHERE d.referenced_type in ('TABLE', 'VIEW') and d.TYPE = 'VIEW'
                """
            )
            for row in cur.mappings().all():
                yield Dependency(
                    name=row.get("name").lower(),
                    referenced_owner=row.get("referenced_owner"),
                    referenced_name=row.get("referenced_name").lower(),
                    referenced_type=DependencyType(row.get("referenced_type")),
                )

    def _get_comment(self, table_name: str):
        return self._inspector.get_table_comment(table_name).get("text")

    def _get_columns(self, table_name: str) -> List[Column]:
        return lmap(create_column, self._inspector.get_columns(table_name))

    def _create_engine(self) -> db.engine.Engine:
        config = self._config
        if config.thick_mode:
            oracledb.init_oracle_client()
        connection_str = f"oracle+cx_oracle://{config.user}:{config.password.get_secret_value()}@{config.host}:{config.port}/?service_name={config.service}"
        return db.create_engine(connection_str)
