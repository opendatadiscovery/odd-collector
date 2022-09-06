from odd_collector.adapters.clickhouse.domain import IntegrationEngine


def test_integration_engine_init():
    response = ("table", "name", "foo")
    engine = IntegrationEngine(*response)

    assert engine.name == "name"
    assert engine.table == "table"
    assert engine.settings == "foo"
