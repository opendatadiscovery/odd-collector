from typing import Any, Dict, List, Optional

from funcy import lmap, lpluck
from odd_models.models import MetadataExtension

# TODO!: Add segments
# TODO!: Add joins
# TODO!: Add pre_aggregations
from ..generator import Generator
from .dimension import Dimension
from .measure import Measure


class Cube:
    def __init__(
        self,
        name: str,
        title: str,
        measures: List[Measure],
        dimensions: List[Dimension],
        segments: List[Any],
        sql: str,
        file_name: Optional[str],
        joins: List[Any],
        pre_aggregations: List[Any],
    ):
        self.name = name
        self.title = title
        self.measures = measures
        self.dimensions = dimensions
        self.segments = segments
        self.sql = sql
        self.file_name = file_name
        self.joins = joins
        self.pre_aggregations = pre_aggregations

    @staticmethod
    def from_response(response: Dict[str, Any]):
        return Cube(
            name=response.get("name"),
            title=response.get("title"),
            measures=lmap(Measure.from_response, response.get("measures")),
            dimensions=lmap(Dimension.from_response, response.get("dimensions")),
            segments=response.get("segments"),
            sql=response.get("sql"),
            file_name=response.get("fileName"),
            joins=response.get("joins"),
            pre_aggregations=response.get("preAggregations"),
        )

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("cubes", self.name)
        return oddrn_generator.get_oddrn_by_path("cubes")
