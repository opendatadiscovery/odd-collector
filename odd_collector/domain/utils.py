from copy import deepcopy
from typing import List
import itertools
import re

from odd_models.models import DataTransformer
from odd_models.utils import SqlParser
from oddrn_generator import Generator

import logging


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


class JustAnotherParser:
    def __init__(self, sql_query: str):
        self.sql_query = sql_query

    @staticmethod
    def patch_query(str_sql) -> str:
        stage1 = str_sql.replace("\n", " ").replace(";", "")
        stage2 = re.sub(r"\s+", " ", stage1).strip()
        stage3 = re.sub(r"(\s*,\s*)", ",", stage2)

        return stage3

    def get_tables_names(self) -> List[str]:
        fmt_sql = self.patch_query(self.sql_query)

        PATTERN = re.compile(
            r"(?:FROM|JOIN)(?:\s+)([^\s\(\)]+)", flags=re.IGNORECASE | re.UNICODE
        )

        regex_matches = [match for match in re.findall(PATTERN, fmt_sql)]
        split_matches = [i.split(",") for i in regex_matches]
        flattened_split_matches = [i for i in itertools.chain(*split_matches)]

        after_from_before_keyword = [
            match for match in re.findall(r"(?:FROM)(.*)(?:WHERE|JOIN)", fmt_sql)
        ]
        sub_keyword_for_comma = [
            segment.replace(",", " FROM ") for segment in after_from_before_keyword
        ]
        sub_regex_matches = [
            re.findall(PATTERN, self.patch_query(i)) for i in sub_keyword_for_comma
        ]

        flattened_sub_matches = [
            i
            for i in itertools.chain(*sub_regex_matches)
            if i not in flattened_split_matches
        ]

        return [*flattened_split_matches, *flattened_sub_matches]


