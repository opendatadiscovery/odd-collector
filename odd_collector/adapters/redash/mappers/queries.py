from odd_collector.domain.utils import AnotherSqlParser
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from ..domain.query import Query
from typing import List
from oddrn_generator.utils.external_generators import (
    DeepLvlGenerator,
    ExternalDbGenerator,
)
from oddrn_generator import RedashGenerator


def create_dataset(oddrn_generator, query: Query):
    parent_oddrn = oddrn_generator.get_oddrn_by_path("queries")
    columns = [map_query(oddrn_generator, column) for column in query.columns]

    return DataSet(parent_oddrn=parent_oddrn, field_list=columns)


class TablesExtractionError(Exception):
    def __init__(self, message):
        super().__init__(message)


def map_query(
    oddrn_generator: RedashGenerator,
    query: Query,
    external_ds_type: ExternalDbGenerator = None,
) -> DataEntity:
    data_entity = DataEntity(
        oddrn=query.get_oddrn(oddrn_generator),
        name=query.name,
        type=DataEntityType.VIEW,
        metadata=query.metadata
        # dataset=create_dataset(oddrn_generator, query),
    )
    parser = AnotherSqlParser(query.metadata[0].metadata.get("query"))
    oddrns: List[str] = []
    tables = parser.get_tables_names()
    for table in tables:
        splitted_table = table.split(".")
        if isinstance(external_ds_type, DeepLvlGenerator):
            if not len(splitted_table) == 2:
                raise TablesExtractionError("absent of schema or table for DeepLvlType")
            schema_name = splitted_table[0]
            table_name = splitted_table[1]
        else:
            if not len(splitted_table) == 1:
                raise TablesExtractionError("multiple values for ShallowLvlType")
            schema_name = ""
            table_name = splitted_table[0]

        view_gen = external_ds_type.get_generator_for_table_lvl(schema_name, table_name)
        oddrns.append(view_gen.get_oddrn_by_path(external_ds_type.table_path_name))
    data_entity.data_transformer = DataTransformer(
        inputs=oddrns, outputs=[], sql=query.metadata[0].metadata.get("query")
    )

    return data_entity
