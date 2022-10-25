import logging
from copy import deepcopy
from typing import List

from odd_models.models import DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import Generator


def extract_transformer_data(
    sql: str, oddrn_generator: Generator, datasets_node_name: str
) -> DataTransformer:
    """
    datasets_node_name: the name of datasets node from PathsModel. for example 'tables', 'datasets' so on

    """
    if type(sql) == bytes:
        sql = sql.decode("utf-8")
    sql_parser = SqlParser(sql.replace("(", "").replace(")", ""))

    try:
        inputs, outputs = sql_parser.get_response()
    except Exception as e:
        logging.error(f"Couldn't parse inputs and outputs from {sql}")
        return DataTransformer(sql=sql, inputs=[], outputs=[])

    return DataTransformer(
        inputs=get_oddrn_list(inputs, oddrn_generator, datasets_node_name),
        outputs=get_oddrn_list(outputs, oddrn_generator, datasets_node_name),
        sql=sql,
    )


def get_oddrn_list(
    tables, oddrn_generator: Generator, datasets_node_name: str
) -> List[str]:
    response = []
    oddrn_generator = deepcopy(oddrn_generator)
    for table in tables:
        source = table.split(".")
        table_name = source[1] if len(source) > 1 else source[0]
        response.append(
            oddrn_generator.get_oddrn_by_path(
                datasets_node_name, table_name.replace("`", "")
            )
        )
    return response
