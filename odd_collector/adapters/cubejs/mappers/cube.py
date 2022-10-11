from odd_models.models import DataConsumer, DataEntity, DataEntityType

from odd_collector.domain.predefined_data_source import PredefinedDataSource

from ..domain.cube import Cube
from ..generator import CubeJsGenerator
from .metadata import map_metadata


def map_cube(
    oddrn_generator: CubeJsGenerator, datasource: PredefinedDataSource, cube: Cube
) -> DataEntity:
    return DataEntity(
        oddrn=cube.get_oddrn(oddrn_generator),
        name=cube.name,
        metadata=[map_metadata(cube)],
        tags=None,
        type=DataEntityType.FILE,
        data_consumer=DataConsumer(inputs=datasource.get_inputs_oddrn(cube.sql)),
    )
