from typing import Any, Dict


class Dimension:
    def __init__(
        self,
        name: str,
        title: str,
        dimension_type: str,
        short_title: str,
        suggest_filter_values: bool,
        is_visible: bool,
        sql: str,
    ):
        self.name = name
        self.title = title
        self.dimension_type = dimension_type
        self.short_title = short_title
        self.suggest_filter_values = suggest_filter_values
        self.is_visible = is_visible
        self.sql = sql

    @staticmethod
    def from_response(response: Dict[str, Any]):
        return Dimension(
            name=response.get("name"),
            title=response.get("title"),
            dimension_type=response.get("type"),
            short_title=response.get("shortTitle"),
            suggest_filter_values=response.get("suggestFilterValues"),
            is_visible=response.get("isVisible"),
            sql=response.get("sql"),
        )
