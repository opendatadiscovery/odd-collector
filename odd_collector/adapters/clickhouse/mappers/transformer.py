from copy import deepcopy
from typing import Dict, List

from odd_models.models import DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import ClickHouseGenerator

from ..domain.integration_engine import IntegrationEngine
from ..domain.table import Table
from .integrated_engines import parse_from_intergated_engine


def extract_transformer_data(
    table: Table,
    oddrn_generator: ClickHouseGenerator,
    integration_engines: List[IntegrationEngine],
) -> DataTransformer:
    sql_parser = SqlParser(table.create_table_query)
    inputs, outputs = sql_parser.get_response()
    integrated_engines_dict = {e.table: e for e in integration_engines}

    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator, table, integrated_engines_dict),
        outputs=get_oddrn_list(
            outputs, oddrn_generator, table, integrated_engines_dict
        ),
        sql=table.create_table_query,
    )


def get_oddrn_list(
    tables: List[str],
    oddrn_generator: ClickHouseGenerator,
    mtable: Table,
    integrated_engines_dict: Dict[str, IntegrationEngine],
) -> List[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)

    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        if mtable.engine == "MaterializedView":
            # if view is configured for a table that reads data from another source, generate oddrn for this source
            ret = parse_from_intergated_engine(
                source_table=table_name, intgr_engine_dict=integrated_engines_dict
            )
            if ret is not None:
                response.extend(ret)
                continue
        response.append(oddrn_generator.get_oddrn_by_path("tables", table_name))
    return response
