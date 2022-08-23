from typing import List

from odd_models.models import DataConsumer, DataEntity, DataEntityType
from odd_models.utils import SqlParser

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
        data_consumer=DataConsumer(inputs=get_inputs(datasource, cube.sql)),
    )


def get_inputs(datasource: PredefinedDataSource, sql: str):
    inputs_tables_names = _get_inputs(sql)
    return [
        generate_oddrn_for_datasource(datasource, table_name)
        for table_name in inputs_tables_names
    ]


def generate_oddrn_for_datasource(datasource: PredefinedDataSource, input: str):
    schema, table = input.split(".")
    return datasource.get_oddrn_for(**{"schemas": schema, "tables": table})


def _get_inputs(sql: str) -> List[str]:
    """
    Args:
        sql: Sql response from Cube has quotas, i.e "`SELECT * FROM public.company`"

    Returns:
        List of tables (with schema) names with schemas used by statement: ['public.computer']

    """
    inputs, _ = SqlParser(sql=sql.strip("`")).get_response()
    return inputs
