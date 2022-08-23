from odd_collector.adapters.cubejs.domain import Cube
from odd_collector.adapters.cubejs.domain.dimension import Dimension
from odd_collector.adapters.cubejs.domain.measure import Measure


def test_cube_response_parsing():
    response = {
        "name": "Company",
        "title": "Company",
        "measures": [
            {
                "name": "Company.count",
                "title": "Company Count",
                "shortTitle": "Count",
                "cumulativeTotal": False,
                "cumulative": False,
                "type": "number",
                "aggType": "count",
                "drillMembers": ["Company.name"],
                "drillMembersGrouped": {"measures": [], "dimensions": ["Company.name"]},
                "isVisible": False,
            }
        ],
        "dimensions": [
            {
                "name": "Company.name",
                "title": "Company Name",
                "type": "string",
                "shortTitle": "Name",
                "suggestFilterValues": True,
                "isVisible": True,
                "sql": "`name`",
            }
        ],
        "segments": [],
        "sql": "`SELECT * FROM public.company`",
        "fileName": "Company.js",
        "joins": [],
        "preAggregations": [],
    }

    cube = Cube.from_response(response)

    assert cube.name == "Company"
    assert cube.title == "Company"
    assert len(cube.measures) == 1
    assert isinstance(cube.measures[0], Measure)
    assert len(cube.dimensions) == 1
    assert isinstance(cube.dimensions[0], Dimension)
    assert cube.segments == []
    assert cube.sql == "`SELECT * FROM public.company`"
    assert cube.joins == []
    assert cube.pre_aggregations == []
