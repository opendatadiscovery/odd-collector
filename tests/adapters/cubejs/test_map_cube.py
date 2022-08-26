import pytest

from odd_collector.adapters.cubejs.domain import Cube, Dimension
from odd_collector.adapters.cubejs.generator import CubeJsGenerator
from odd_collector.adapters.cubejs.mappers.cube import map_cube
from odd_collector.domain.plugin import CubeJSPlugin
from odd_collector.domain.predefined_data_source import (
    PostgresDatasource,
    PredefinedDatasourceParams,
)


@pytest.fixture
def generator():
    return CubeJsGenerator(host_settings="host")


@pytest.fixture
def data_source():
    params = PredefinedDatasourceParams(
        type="postgres", database="computer", host="localhost"
    )
    return PostgresDatasource(params)


def test_map_cube(generator, data_source):
    cube = Cube(
        name="some_name",
        title="title",
        measures=[],
        dimensions=[
            Dimension(
                name="PassInTrip.place",
                title="Pass in Trip Place",
                dimension_type="string",
                short_title="Place",
                suggest_filter_values=True,
                is_visible=True,
                sql="`place`",
            ),
            Dimension(
                name="PassInTrip.date",
                title="Pass in Trip Date",
                dimension_type="time",
                short_title="Date",
                suggest_filter_values=True,
                is_visible=True,
                sql="`date`",
            ),
        ],
        segments=[],
        sql="`SELECT * FROM public.company`",
        file_name="Test.js",
        joins=[],
        pre_aggregations=[],
    )

    data_entity = map_cube(generator, data_source, cube)

    assert data_entity.oddrn == "//cubejs/host/host/cubes/some_name"
    assert data_entity.name == "some_name"
    assert len(data_entity.data_consumer.inputs) == 1
    assert (
        data_entity.data_consumer.inputs[0]
        == "//postgresql/host/localhost/databases/computer/schemas/public/tables/company"
    )
    assert data_entity.metadata is not None
    assert len(data_entity.metadata) == 1
    assert data_entity.metadata[0].metadata.get("fileName") == "Test.js"
    assert (
        data_entity.metadata[0].metadata.get("dimensions")
        == "PassInTrip.place, PassInTrip.date"
    )
