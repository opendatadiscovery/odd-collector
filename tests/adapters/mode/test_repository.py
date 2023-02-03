import pytest
from odd_models.models import DataEntityType

from odd_collector.adapters.mode.adapter import Adapter
from odd_collector.adapters.mode.domain.collection import Collection
from odd_collector.adapters.mode.domain.datasource import DataSource
from odd_collector.adapters.mode.domain.query import Query
from odd_collector.adapters.mode.domain.report import Report
from odd_collector.adapters.mode.repository import ModeRepository
from odd_collector.domain.plugin import ModePlugin
from tests.adapters.mode.mock_rest_client import TestRestClient


@pytest.fixture()
def setup_repository():
    config = ModePlugin(
        name="mode",
        type="mode",
        host="host",
        account="account",
        data_source="test",
        token="token",
        password="password",
    )
    repository = ModeRepository(config)
    repository.rest_client = TestRestClient()
    return repository


def compare_dict_with_object(dict_object, class_object):
    for class_attribute, model_field in class_object.__fields__.items():
        attribute_value = getattr(class_object, class_attribute)
        if class_attribute in dict_object.keys():
            expected_value = dict_object[class_attribute]
            if expected_value is not None:
                expected_value = type(attribute_value)(expected_value)
            assert expected_value == attribute_value
        else:
            assert attribute_value is None


@pytest.mark.asyncio
async def test_get_data_sources(setup_repository):
    result = await setup_repository.get_data_sources()
    expected_ds_list = setup_repository.rest_client.data_sources["_embedded"][
        "data_sources"
    ]
    data_source = result[0]
    assert len(result) == len(expected_ds_list)
    assert data_source.__class__ == DataSource
    compare_dict_with_object(expected_ds_list[0], data_source)


@pytest.mark.asyncio
async def test_get_collections(setup_repository):
    result = await setup_repository.get_collections()
    expected_ds_list = setup_repository.rest_client.collections["_embedded"]["spaces"]
    collection = result[0]
    assert len(result) == len(expected_ds_list)
    assert collection.__class__ == Collection
    compare_dict_with_object(expected_ds_list[0], collection)


@pytest.mark.asyncio
async def test_get_queries_for_reports(setup_repository):
    report_token = setup_repository.rest_client.reports["_embedded"]["reports"][0][
        "token"
    ]
    result = await setup_repository.get_queries_for_reports(report_token)
    expected_query_list = setup_repository.rest_client.queries["_embedded"]["queries"]
    query = result[0]
    assert len(result) == len(expected_query_list)
    assert query.__class__ == Query
    compare_dict_with_object(expected_query_list[0], query)


@pytest.mark.asyncio
async def test_get_reports_for_data_source(setup_repository):
    ds = DataSource.from_response(
        setup_repository.rest_client.data_sources["_embedded"]["data_sources"][0]
    )
    result = await setup_repository.get_reports_for_data_source(ds)
    report = result[0]
    assert report.__class__ == Report

    expected_report_list = setup_repository.rest_client.reports["_embedded"]["reports"]
    expected_report_list[0]["host"] = ds.host
    expected_report_list[0]["database"] = ds.database
    expected_report_list[0]["adapter"] = ds.adapter
    assert len(result) == len(expected_report_list)
    assert type(report.queries) is list
    assert all([isinstance(item, Query) for item in report.queries])
    for json, item in zip(
        setup_repository.rest_client.queries["_embedded"]["queries"], report.queries
    ):
        compare_dict_with_object(json, item)
    # small workaround, after verifying report queries we can skip there verification
    report.queries = None
    compare_dict_with_object(expected_report_list[0], report)


@pytest.mark.asyncio
async def test_get_reports_for_space(setup_repository):
    space = Collection.from_response(
        setup_repository.rest_client.collections["_embedded"]["spaces"][0]
    )
    result = await setup_repository.get_reports_for_space(space)
    report = result[0]
    assert report.__class__ == Report

    expected_report_list = setup_repository.rest_client.reports["_embedded"]["reports"]
    assert len(result) == len(expected_report_list)
    assert type(report.queries) is list
    assert all([isinstance(item, Query) for item in report.queries])
    for json, item in zip(
        setup_repository.rest_client.queries["_embedded"]["queries"], report.queries
    ):
        compare_dict_with_object(json, item)
    # small workaround, after verifying report queries we can skip there verification
    report.queries = None
    compare_dict_with_object(expected_report_list[0], report)


@pytest.mark.asyncio
async def test_get_data_entity_list():
    config = ModePlugin(
        name="mode",
        type="mode",
        host="https://test.com",
        account="account",
        data_source="test",
        token="token",
        password="password",
    )
    repository = ModeRepository
    adapter = Adapter(config, repository)
    adapter.repo.rest_client = TestRestClient()
    data_entity_list = await adapter.get_data_entity_list()
    assert len(data_entity_list.items) == 1
    assert data_entity_list.items[0].type == DataEntityType.DASHBOARD
    assert data_entity_list.items[0].data_consumer.inputs == [
        "//mssql/host/ec2-54-190-97-226.us-west-2.compute.amazonaws.com/databases/"
        "AdventureWorks/schemas/Person/tables/StateProvince"
    ]
