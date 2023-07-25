import sqlalchemy.sql.sqltypes as sqltype
from odd_models.models import Type


def map_type(column_type: sqltype) -> Type:
    if isinstance(column_type, sqltype.Numeric):
        return Type.TYPE_NUMBER
    elif isinstance(column_type, sqltype.Integer):
        return Type.TYPE_INTEGER
    elif isinstance(column_type, (sqltype.DateTime, sqltype.Date)):
        return Type.TYPE_DATETIME
    elif isinstance(column_type, (sqltype.Time,)):
        return Type.TYPE_TIME
    elif isinstance(column_type, sqltype.Text):
        return Type.TYPE_CHAR
    elif isinstance(column_type, sqltype.String):
        return Type.TYPE_STRING
    elif isinstance(column_type, sqltype._Binary):
        return Type.TYPE_BINARY
    elif isinstance(column_type, sqltype.Boolean):
        return Type.TYPE_BOOLEAN
    else:
        return Type.TYPE_UNKNOWN
