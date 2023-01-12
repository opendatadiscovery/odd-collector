from typing import List, Iterable

import pytest
from odd_models.models import DataEntityType

from odd_collector.adapters.mssql.adapter import Adapter
from odd_collector.adapters.mssql.models import Table, View
from odd_collector.adapters.mssql.repository import DefaultConnector, Columns
from odd_collector.domain.plugin import MSSQLPlugin


class TestDefaultConnector(DefaultConnector):
    _table_response = [
        ('AdventureWorks', 'HumanResources', 'Employee', 'BASE TABLE')
    ]

    _column_response = [
        (
        'AdventureWorks', 'HumanResources', 'Employee', 'BusinessEntityID', 1, None, 'NO', 1, 'int', None, None, 10, 10,
        0, None, None, None, None, None, None, None, None, None, None),
        ('AdventureWorks', 'HumanResources', 'Employee', 'NationalIDNumber', 2, None, 'NO', 0, 'nvarchar', 15, 30, None,
         None, None, None, None, None, 'UNICODE', None, None, 'SQL_Latin1_General_CP1_CI_AS', None, None, None),
        ('AdventureWorks', 'HumanResources', 'Employee', 'LoginID', 3, None, 'NO', 0, 'nvarchar', 256, 512, None, None,
         None, None, None, None, 'UNICODE', None, None, 'SQL_Latin1_General_CP1_CI_AS', None, None, None),
        ('AdventureWorks', 'HumanResources', 'Employee', 'OrganizationNode', 4, None, 'YES', 0, 'hierarchyid', 892, 892,
         None, None, None, None, None, None, None, None, None, None, None, None, None),
        ('AdventureWorks', 'HumanResources', 'Employee', 'OrganizationLevel', 5, None, 'YES', 0, 'smallint', None, None,
         5, 10, 0, None, None, None, None, None, None, None, None, None, None),
        ('AdventureWorks', 'HumanResources', 'Employee', 'JobTitle', 6, None, 'NO', 0, 'nvarchar', 50, 100, None, None,
         None, None, None, None, 'UNICODE', None, None, 'SQL_Latin1_General_CP1_CI_AS', None, None, None),
    ]

    def get_tables(self) -> Iterable[Table]:
        for row in self._table_response:
            yield Table(*row)

    def get_columns(self) -> Columns:
        return Columns(self._column_response)

    def get_views(self) -> Iterable[View]:
        return []


@pytest.fixture
def client():
    return TestDefaultConnector


@pytest.fixture
def config() -> MSSQLPlugin:
    return MSSQLPlugin(
        name="mssql_adapter",
        description="mssql_adapter",
        namespace="",
        type="mssql",
        host="localhost",
        port=1433,
        database="Test",
        user="user",
        password="password",
    )


def test_primary_keys(config, client):
    adapter = Adapter(config)
    adapter._repo = client(config)
    data_entity_list = adapter.get_data_entity_list()

    table_elements = [de for de in data_entity_list.items if de.type == DataEntityType.TABLE]
    employee = table_elements[0]
    columns = employee.dataset.field_list

    assert columns[0].is_primary_key
    for column in columns[1:]:
        assert not column.is_primary_key
