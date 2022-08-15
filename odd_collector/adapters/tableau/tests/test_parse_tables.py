from odd_collector.adapters.tableau.domain.table import (
    BigqueryTable,
    EmbeddedTable,
    databases_to_tables,
    traverse_tables,
)


def test_databases_to_tables():
    databases_response = [
        {
            "connectionType": "textscan",
            "id": "02c3936a-8b08-032b-912b-27b83240a123",
            "name": "orders_south_2015.csv",
            "downstreamOwners": [{"name": "pmack"}],
            "tables": [
                {
                    "id": "ef5fc198-8311-5285-d525-3e78b8311c77",
                    "name": "orders_south_2015.csv",
                    "schema": "",
                }
            ],
        },
        {
            "connectionType": "excel-direct",
            "id": "0f1558d0-c481-7b62-c161-f2e620ff67f3",
            "name": "return reasons_new.xlsx",
            "downstreamOwners": [{"name": "pmack"}],
            "tables": [
                {
                    "id": "7a9a6203-7777-0334-9a8e-f995fc1ceb34",
                    "name": "returns_new",
                    "schema": "",
                }
            ],
        },
    ]

    tables = databases_to_tables(databases_response)
    assert len(tables) == 2
    assert tables[0].id == "ef5fc198-8311-5285-d525-3e78b8311c77"
    assert tables[1].id == "7a9a6203-7777-0334-9a8e-f995fc1ceb34"


def test_traverse_tables():
    table_response = {
        "connectionType": "bigquery",
        "id": "15995808-6508-dded-fb32-e7905a6d61d1",
        "name": "publicdata",
        "downstreamOwners": [{"name": "pmakarichev"}],
        "tables": [
            {
                "id": "0c2df5d9-85e6-5682-69be-20b54fbd39d9",
                "name": "github_nested",
                "schema": "samples",
                "description": "",
            },
            {
                "id": "29e00772-c836-b5b1-e726-afa64dc72251",
                "name": "natality",
                "schema": "samples",
                "description": "dataset",
            },
        ],
    }

    tables = traverse_tables(table_response)

    assert len(tables) == 2
    table = tables[0]
    assert table.id == "0c2df5d9-85e6-5682-69be-20b54fbd39d9"
    assert table.name == "github_nested"
    assert table.connection_type == "bigquery"
    assert table.database_name == "publicdata"
    assert table.database_id == "15995808-6508-dded-fb32-e7905a6d61d1"
    assert table.schema == "samples"
    assert table.description is None
    assert table.owners == ["pmakarichev"]
    assert isinstance(table, BigqueryTable)

    table = tables[1]
    assert table.id == "29e00772-c836-b5b1-e726-afa64dc72251"
    assert table.name == "natality"
    assert table.connection_type == "bigquery"
    assert table.database_name == "publicdata"
    assert table.database_id == "15995808-6508-dded-fb32-e7905a6d61d1"
    assert table.schema == "samples"
    assert table.description == "dataset"
    assert table.owners == ["pmakarichev"]
    assert isinstance(table, BigqueryTable)


def test_parse_bigquery_table():
    response = {
        "connectionType": "textscan",
        "id": "02c3936a-8b08-032b-912b-27b83240a123",
        "name": "orders_south_2015.csv",
        "downstreamOwners": [{"name": "pmack"}],
        "tables": [
            {
                "id": "ef5fc198-8311-5285-d525-3e78b8311c77",
                "name": "orders_south_2015.csv",
                "schema": "",
            }
        ],
    }

    tables = traverse_tables(response)

    assert len(tables) == 1
    table = tables[0]
    assert isinstance(table, EmbeddedTable)
