from odd_collector.adapters.tableau.domain.column import Column


def test_column_from_response():
    column_response = {
        "id": "77b5f158-1c74-7cc7-f9f0-89b9044581ee",
        "isNullable": True,
        "name": "Category",
        "remoteType": "WSTR",
        "description": "PII Column",
    }

    column = Column.from_response(column_response)

    assert column.name == "Category"
    assert column.is_nullable == True
    assert column.remote_type == "WSTR"
    assert column.id == "77b5f158-1c74-7cc7-f9f0-89b9044581ee"
    assert column.description == "PII Column"

    column_response = {
        "id": "77b5f158-1c74-7cc7-f9f0-89b9044581ee",
        "isNullable": True,
        "name": "Category",
        "remoteType": "WSTR",
        "description": "",
    }

    column = Column.from_response(column_response)

    assert column.name == "Category"
    assert column.is_nullable == True
    assert column.remote_type == "WSTR"
    assert column.id == "77b5f158-1c74-7cc7-f9f0-89b9044581ee"
    assert column.description is None
