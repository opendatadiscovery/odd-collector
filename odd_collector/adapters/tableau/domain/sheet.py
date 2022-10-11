from typing import Any, List, Optional

from funcy import cat, get_in, ldistinct, pluck


class Sheet:
    def __init__(
        self,
        id: str,
        name: str,
        workbook: str,
        owner: str,
        created: str,
        updated: str,
        table_ids: Optional[List[Any]],
    ):
        self.id = id
        self.name = name
        self.workbook = workbook
        self.owner = owner
        self.created = created
        self.updated = updated
        self.tables_id = table_ids or []

    @staticmethod
    def from_response(response):
        tables = cat(pluck("upstreamTables", response.get("upstreamFields")))
        table_ids = ldistinct(pluck("id", tables))

        return Sheet(
            id=response.get("id"),
            name=response.get("name"),
            workbook=get_in(response, ["workbook", "name"]),
            owner=get_in(response, ["workbook", "owner", "name"]),
            table_ids=table_ids,
            created=response.get("createdAt"),
            updated=response.get("updatedAt"),
        )
