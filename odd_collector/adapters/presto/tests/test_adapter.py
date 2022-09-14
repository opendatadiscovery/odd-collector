from odd_collector.adapters.presto.presto_repository_base import PrestoRepositoryBase
from odd_collector.adapters.presto.adapter import Adapter
from .raw_data import tables_nodes, columns_nodes, nested_nodes
from odd_collector.domain.plugin import PrestoPlugin


class PrestoRepositoryTest(PrestoRepositoryBase):
    def get_columns(self):
        return columns_nodes

    def get_tables(self):
        return tables_nodes


presto_plugin = PrestoPlugin(
    type="presto", host="localhost", port=8081, user="presto", name="presto_adapter"
)

ad = Adapter(presto_plugin, PrestoRepositoryTest)


def test_get_nested_nodes():
    assert ad.get_nested_columns_nodes() == nested_nodes


def test_get_data_source_oddrn():
    assert ad.get_data_source_oddrn() == "//presto/host/localhost:8081"
