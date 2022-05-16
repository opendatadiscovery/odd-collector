from odd_models.models import Type

__TYPES_SQL_TO_ODD: dict[str, Type] = {
    "I1": Type.TYPE_INTEGER,
    "I2": Type.TYPE_INTEGER,
    "I4": Type.TYPE_INTEGER,
    "I8": Type.TYPE_INTEGER,
    "UI1": Type.TYPE_INTEGER,
    "UI2": Type.TYPE_INTEGER,
    "UI4": Type.TYPE_INTEGER,
    "UI8": Type.TYPE_INTEGER,
    "R4": Type.TYPE_NUMBER,
    "R8": Type.TYPE_NUMBER,
    "DECIMAL": Type.TYPE_NUMBER,
    "NUMERIC": Type.TYPE_NUMBER,
    "CY": Type.TYPE_NUMBER,
    "VARNUMERIC": Type.TYPE_NUMBER,
    "BOOL": Type.TYPE_BOOLEAN,
    "BSTR": Type.TYPE_CHAR,
    "STR": Type.TYPE_STRING,
    "WSTR": Type.TYPE_STRING,
    "DATE": Type.TYPE_DATETIME,
    "DBDATE": Type.TYPE_DATETIME,
    "DBTIMESTAMP": Type.TYPE_DATETIME,
    "FILETIME": Type.TYPE_DATETIME,
    "DBTIME": Type.TYPE_TIME,
    "BYTES": Type.TYPE_BINARY,
    "GUID": Type.TYPE_STRING,
    "XML": Type.TYPE_STRING,
    "IDISPATCH": Type.TYPE_BINARY,
    "ERROR": Type.TYPE_BINARY,
    "VARIANT": Type.TYPE_BINARY,
    "IUNKNOWN": Type.TYPE_BINARY,
    "HCHAPTER": Type.TYPE_BINARY,
    "PROPVARIANT": Type.TYPE_BINARY,
    "BYREF": Type.TYPE_BINARY,
    "UDT": Type.TYPE_STRUCT,
    "ARRAY": Type.TYPE_LIST,
    "VECTOR": Type.TYPE_LIST,
}


def map_type(tableau_type: str, default: Type = Type.TYPE_UNKNOWN):
    return __TYPES_SQL_TO_ODD.get(tableau_type, default)
