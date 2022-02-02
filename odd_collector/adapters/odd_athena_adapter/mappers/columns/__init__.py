import logging
from typing import List, Dict, Any

from lark import Lark, LarkError
from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import AthenaGenerator

from .athena_field_type_transformer import AthenaFieldTypeTransformer

athena_field_type_transformer = AthenaFieldTypeTransformer()
parser = Lark.open('grammar/athena_field_type_grammar.lark', rel_to=__file__, parser="lalr", start='type')

TYPES_ATHENA_TO_ODD = {
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


def __parse(field_type: str) -> Dict[str, Any]:
    column_tree = parser.parse(field_type)
    return athena_field_type_transformer.transform(column_tree)


def __map_column(oddrn_generator: AthenaGenerator,
                 type_parsed: Dict[str, Any],
                 parent_oddrn: str = None,
                 parent_oddrn_path: str = "tables",
                 column_name: str = None,
                 column_description: str = None,
                 is_key: bool = None,
                 is_value: bool = None
                 ) -> List[DataSetField]:
    result = []
    athena_type = type_parsed['type']
    name = column_name if column_name is not None \
        else type_parsed["field_name"] if "field_name" in type_parsed \
        else athena_type

    resource_name = "keys" if is_key \
        else "values" if is_value \
        else "subcolumns"

    oddrn = (
        oddrn_generator.get_oddrn_by_path(f'{parent_oddrn_path}_columns', name)
        if parent_oddrn is None else f'{parent_oddrn}/{resource_name}/{name}'
    )
    dsf = DataSetField(
        name=name,
        oddrn=oddrn,
        parent_field_oddrn=parent_oddrn,
        type=DataSetFieldType(
            type=TYPES_ATHENA_TO_ODD[athena_type],
            logical_type=athena_type,
            is_nullable=True
        ),
        is_key=bool(is_key),
        is_value=bool(is_value),
        owner=None,
        metadata=[],
        default_value=None,
        description=column_description
    )

    if athena_type in ['list', 'struct', 'union']:
        for children in type_parsed['children']:
            result.extend(__map_column(oddrn_generator=oddrn_generator,
                                       parent_oddrn=dsf.oddrn,
                                       type_parsed=children))

    if athena_type == 'map':
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
               oddrn_generator: AthenaGenerator,
               parent_oddrn_path) -> List[DataSetField]:
    try:
        type_parsed = __parse(raw_column_data['Type'])

        return __map_column(oddrn_generator=oddrn_generator,
                            column_name=raw_column_data['Name'],
                            column_description=raw_column_data.get('Comment'),
                            type_parsed=type_parsed,
                            parent_oddrn_path=parent_oddrn_path)
    except LarkError as e:
        logging.warning("There was an error during type parsing. "
                        f"Column name: \"{raw_column_data['Name']}\". "
                        f"Table oddrn: \"{oddrn_generator.get_oddrn_by_path('tables')}\". "
                        f"Reason: \"{e}\"")
        return []
