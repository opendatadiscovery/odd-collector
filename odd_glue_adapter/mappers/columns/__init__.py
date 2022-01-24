import logging
from typing import List, Tuple, Dict, Any, Iterable

from lark import Lark, LarkError
from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import GlueGenerator

from .field_stat_schema import FIELD_TYPE_SCHEMA
from .glue_field_type_transformer import GlueFieldTypeTransformer

glue_field_type_transformer = GlueFieldTypeTransformer()
parser = Lark.open('grammar/glue_field_type_grammar.lark', rel_to=__file__, parser="lalr", start='type')

TYPES_GLUE_TO_ODD = {
    'int': 'TYPE_INTEGER',
    'smallint': 'TYPE_INTEGER',
    'timestamp': 'TYPE_INTEGER',
    'tinyint': 'TYPE_INTEGER',

    'double': 'TYPE_NUMBER',
    'bigint': 'TYPE_NUMBER',
    'decimal': 'TYPE_NUMBER',
    'float': 'TYPE_NUMBER',

    'string': 'TYPE_STRING',
    'varchar': 'TYPE_STRING',

    'boolean': 'TYPE_BOOLEAN',
    'char': 'TYPE_CHAR',
    'date': 'TYPE_DATETIME',
    'interval': 'TYPE_DURATION',
    'binary': 'TYPE_BINARY',

    'list': 'TYPE_LIST',
    'map': 'TYPE_MAP',
    'struct': 'TYPE_STRUCT',
    'union': 'TYPE_UNION'
}


def __map_glue_type(glue_type: str) -> str:
    return TYPES_GLUE_TO_ODD.get(glue_type, 'TYPE_UNKNOWN')


def __parse(field_type: str) -> Dict[str, Any]:
    column_tree = parser.parse(field_type)
    return glue_field_type_transformer.transform(column_tree)


def __map_column(oddrn_generator: GlueGenerator,
                 type_parsed: Dict[str, Any],
                 parent_oddrn: str = None,
                 column_name: str = None,
                 column_description: str = None,
                 stats: DataSetField = None,
                 is_key: bool = None,
                 is_value: bool = None
                 ) -> List[DataSetField]:
    result = []
    glue_type = type_parsed['type']
    name = column_name if column_name is not None \
        else type_parsed["field_name"] if "field_name" in type_parsed \
        else glue_type

    resource_name = "keys" if is_key \
        else "values" if is_value \
        else "subcolumns"

    dsf = DataSetField(
        name=name,
        oddrn=oddrn_generator.get_oddrn_by_path('columns', name) if parent_oddrn is None else f'{parent_oddrn}/{resource_name}/{name}',
        parent_field_oddrn=parent_oddrn,
        type=DataSetFieldType(
            type=__map_glue_type(glue_type),
            logical_type=type_parsed.get('logical_type', glue_type),
            is_nullable=True
        ),
        is_key=bool(is_key),
        is_value=bool(is_value),
        owner=None,
        metadata=[],
        stats=stats or None,
        default_value=None,
        description=column_description
    )

    if glue_type in ['list', 'struct', 'union']:
        for children in type_parsed['children']:
            result.extend(__map_column(oddrn_generator=oddrn_generator,
                                       parent_oddrn=dsf.oddrn,
                                       type_parsed=children))

    if glue_type == 'map':
        result.extend(__map_column(
            oddrn_generator=oddrn_generator,
            parent_oddrn=dsf.oddrn,
            type_parsed=type_parsed['key_type'],
            is_key=True)
        )

        result.extend(__map_column(
            oddrn_generator=oddrn_generator,
            parent_oddrn=dsf.oddrn,
            type_parsed=type_parsed['value_type'],
            is_value=True)
        )

    result.append(dsf)
    return result


def map_column(raw_column_data: Dict[str, Any],
               oddrn_generator: GlueGenerator,
               stats: DataSetField) -> List[DataSetField]:
    try:
        type_parsed = __parse(raw_column_data['Type'])

        return __map_column(oddrn_generator=oddrn_generator,
                            column_name=raw_column_data['Name'],
                            column_description=raw_column_data.get('Comment'),
                            stats=stats,
                            type_parsed=type_parsed)
    except LarkError as e:
        logging.warning("There was an error during type parsing. "
                        f"Column name: \"{raw_column_data['Name']}\". "
                        f"Table oddrn: \"{oddrn_generator.get_oddrn_by_path('tables')}\". "
                        f"Reason: \"{e}\"")
        return []


def map_column_stats(column_stats: List[Dict[str, Any]]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    return [__map_column_stat(raw_column_stat) for raw_column_stat in column_stats]


def __map_column_stat(raw_column_stat: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    raw_statistics_data = raw_column_stat['StatisticsData']
    statistics_data = FIELD_TYPE_SCHEMA[raw_statistics_data['Type']]
    mapper_fn = statistics_data['mapper']
    glue_type_name = statistics_data['glue_type_name']

    return raw_column_stat['ColumnName'], {
        statistics_data['field_name']: mapper_fn(raw_statistics_data[glue_type_name])
    }
