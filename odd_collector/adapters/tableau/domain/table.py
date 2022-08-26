from typing import Any, List, Optional

from funcy import lmapcat, lpluck

from odd_collector.adapters.tableau.domain.column import Column


class Table:
    def __init__(
        self,
        id: str,
        name: str,
        schema: Optional[str],
        db_id: str,
        db_name: str,
        connection_type: str,
        columns: List[Column] = None,
        owners: List[str] = None,
        description: str = None,
    ):
        self.id = id
        self.name = name
        self.schema = schema or "unknown_schema"
        self.database_name = db_name
        self.database_id = db_id
        self.connection_type = connection_type
        self.columns = columns or []
        self.owners = owners or []
        self.description = description or None

    def get_oddrn(self, oddrn_generator):
        oddrn_generator.set_oddrn_paths(
            databases=self.database_id,
            schemas=self.schema,
            tables=self.name,
        )
        return oddrn_generator.get_oddrn_by_path("tables")


class BigqueryTable(Table):
    def get_oddrn(self, oddrn_generator):
        db_name = self.database_name.lower()
        schema = self.schema
        name = self.name
        return f"//bigquery_storage/cloud/gcp/project/{db_name}/datasets/{schema}/tables/{name}"


class EmbeddedTable(Table):
    pass


def create_table(**kwargs) -> Table:
    """Factory Method"""
    connection_type = kwargs.get("connection_type")
    constructors = {"bigquery": BigqueryTable}
    constructor = constructors.get(connection_type, EmbeddedTable)

    return constructor(**kwargs)


def databases_to_tables(databases_response: List[Any]) -> List[Table]:
    return lmapcat(traverse_tables, databases_response)


def traverse_tables(database_response) -> List[Table]:
    connection_type = database_response.get("connectionType")
    db_name = database_response.get("name")
    db_id = database_response.get("id")
    owners = lpluck("name", database_response.get("downstreamOwners"))

    tables = []

    for table in database_response.get("tables"):
        tbl_id = table.get("id")
        tbl_name = table.get("name")
        tbl_schema = table.get("schema")
        description = table.get("description")

        tables.append(
            create_table(
                id=tbl_id,
                name=tbl_name,
                schema=tbl_schema,
                db_id=db_id,
                db_name=db_name,
                connection_type=connection_type,
                description=description,
                owners=owners,
            )
        )

    return tables
