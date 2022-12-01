from enum import Enum


class EntityTypePathKey(Enum):
    """Helper class for splitting views and tables"""

    TABLE = "tables"
    VIEW = "views"
