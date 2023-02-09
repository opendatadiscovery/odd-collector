from odd_collector.adapters.snowflake.domain.entity import Connectable


def test_connectable():
    raw_object = {"upstream": "db1.public.table.TABLE"}
    connectable = Connectable.parse_obj(raw_object)

    assert len(connectable.upstream) == 1
    connection = connectable.upstream[0]
    assert connection.table_catalog == "db1"
    assert connection.table_schema == "public"
    assert connection.table_name == "table"
    assert connection.domain == "TABLE"
