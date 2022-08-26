from datetime import datetime
from typing import List

import pytest
from dateutil.tz import tzoffset
from odd_models.models import DataEntityType

from odd_collector.domain.plugin import VerticaPlugin
from ..adapter import Adapter
from ..domain.column import Column
from ..domain.table import Table
from ..vertica_repository_base import VerticaRepositoryBase


class VerticaTestRepository(VerticaRepositoryBase):
    _table_response = [
        [
            "public",
            "product_dimension",
            "TABLE",
            "This is my comment for product table",
            45035996273707482,
            "dbadmin",
            datetime(2022, 8, 18, 10, 41, 24, 533958, tzinfo=tzoffset(None, 0)),
            500,
            False,
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            "TABLE",
            None,
            45035996273707478,
            "dbadmin",
            datetime(2022, 8, 18, 10, 41, 24, 492769, tzinfo=tzoffset(None, 0)),
            50000,
            False,
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            "TABLE",
            None,
            45035996273707550,
            "dbadmin",
            datetime(2022, 8, 18, 10, 41, 25, 28915, tzinfo=tzoffset(None, 0)),
            200,
            False,
            False,
            None,
            None,
        ],
        [
            "public",
            "test_view",
            "VIEW",
            None,
            45035996273843144,
            "dbadmin",
            datetime(2022, 8, 18, 11, 50, 48, 675010, tzinfo=tzoffset(None, 0)),
            0,
            None,
            None,
            "SELECT date_dimension.date_key, date_dimension.date, date_dimension.full_date_description, "
            "date_dimension.day_of_week, date_dimension.day_number_in_calendar_month, "
            "date_dimension.day_number_in_calendar_year, "
            "date_dimension.day_number_in_fiscal_month, date_dimension.day_number_in_fiscal_year, "
            "date_dimension.last_day_in_week_indicator, date_dimension.last_day_in_month_indicator, "
            "date_dimension.calendar_week_number_in_year, "
            "date_dimension.calendar_month_name, date_dimension.calendar_month_number_in_year, "
            "date_dimension.calendar_year_month, date_dimension.calendar_quarter, "
            "date_dimension.calendar_year_quarter, "
            "date_dimension.calendar_half_year, date_dimension.calendar_year, date_dimension.holiday_indicator, "
            "date_dimension.weekday_indicator, date_dimension.selling_season FROM public.date_dimension "
            "WHERE (date_dimension.calendar_month_name = 'January'::varchar(7))",
            False,
        ],
    ]

    _column_response = [
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-12",
            "cc_region",
            "varchar",
            False,
            9,
            64,
            64,
            None,
            None,
            None,
            None,
            12,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-10",
            "cc_city",
            "varchar",
            False,
            9,
            64,
            64,
            None,
            None,
            None,
            None,
            10,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-9",
            "cc_address",
            "varchar",
            False,
            9,
            256,
            256,
            None,
            None,
            None,
            None,
            9,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-8",
            "cc_manager",
            "varchar",
            False,
            9,
            40,
            40,
            None,
            None,
            None,
            None,
            8,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-4",
            "cc_name",
            "varchar",
            False,
            9,
            50,
            50,
            None,
            None,
            None,
            None,
            4,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-3",
            "cc_open_date",
            "date",
            False,
            10,
            8,
            None,
            None,
            None,
            None,
            None,
            3,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "online_sales",
            "call_center_dimension",
            False,
            "45035996273707550-1",
            "call_center_key",
            "int",
            True,
            6,
            8,
            None,
            None,
            None,
            None,
            None,
            1,
            False,
            "",
            "",
            False,
            False,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-21",
            "last_deal_update",
            "date",
            False,
            10,
            8,
            None,
            None,
            None,
            None,
            None,
            21,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-18",
            "customer_since",
            "date",
            False,
            10,
            8,
            None,
            None,
            None,
            None,
            None,
            18,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-10",
            "customer_region",
            "varchar",
            False,
            9,
            64,
            64,
            None,
            None,
            None,
            None,
            10,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-5",
            "title",
            "varchar",
            False,
            9,
            8,
            8,
            None,
            None,
            None,
            None,
            5,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-3",
            "customer_name",
            "varchar",
            False,
            9,
            256,
            256,
            None,
            None,
            None,
            None,
            3,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-2",
            "customer_type",
            "varchar",
            False,
            9,
            16,
            16,
            None,
            None,
            None,
            None,
            2,
            True,
            "",
            "",
            False,
            None,
            None,
        ],
        [
            "public",
            "customer_dimension",
            False,
            "45035996273707478-1",
            "customer_key",
            "int",
            True,
            6,
            8,
            None,
            None,
            None,
            None,
            None,
            1,
            False,
            "",
            "",
            False,
            False,
            None,
        ],
    ]

    def __init__(self, config: VerticaPlugin):
        self.config = config

    def get_tables(self) -> List[Table]:
        response = self._table_response
        tables = [Table(*table) for table in response]
        return tables

    def get_columns(self) -> List[Column]:
        response = self._column_response
        columns = [Column(*column) for column in response]
        return columns


@pytest.fixture
def client():
    return VerticaTestRepository


@pytest.fixture
def config() -> VerticaPlugin:
    return VerticaPlugin(
        name="vertica_datasource",
        description="vetica_adapter",
        namespace="",
        type="vertica",
        host="localhost",
        port=5433,
        database="VMart",
        user="user",
        password="password",
    )


def test_adapter(config, client):
    adapter = Adapter(config)
    adapter._Adapter__repository = client(config)
    data_entity_list = adapter.get_data_entity_list()

    table_elements = []
    views_elements = []
    db_service = []
    for de in data_entity_list.items:
        if de.type == DataEntityType.TABLE:
            table_elements.append(de)
        if de.type == DataEntityType.VIEW:
            views_elements.append(de)
        if de.type == DataEntityType.DATABASE_SERVICE:
            db_service.append(de)

    assert len(data_entity_list.items) == 5

    assert len(table_elements) == 3
    customer_dimension = table_elements[1]
    assert len(customer_dimension.dataset.field_list) == 7
    assert customer_dimension.oddrn == customer_dimension.dataset.parent_oddrn
    assert customer_dimension in data_entity_list.items

    assert len(views_elements) == 1
    test_view = views_elements[0]
    assert test_view.dataset.field_list == []

    assert len(db_service) == 1
