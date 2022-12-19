from enum import IntEnum


class ColumnType(IntEnum):
    STRING = 0
    NUMBER = 1
    INTEGER = 2
    BOOLEAN = 3
    CHAR = 4
    DATETIME = 5
    TIME = 6
    BINARY = 7
    UNKNOWN = 8
