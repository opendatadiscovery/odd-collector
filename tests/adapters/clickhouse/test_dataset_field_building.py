import unittest

from odd_models.models import DataSetField, Type
from oddrn_generator import ClickHouseGenerator

from odd_collector.adapters.clickhouse.domain import Column
from odd_collector.adapters.clickhouse.mappers.columns import build_dataset_fields


class TestDataSetFieldsBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.oddrn_generator = ClickHouseGenerator(
            host_settings="test", databases="test", tables="test"
        )

    def test_simple_case(self):
        column = Column(
            database="test",
            table="test",
            name="a",
            type="String",
            position=0,
            default_kind="",
            default_expression="",
            data_compressed_bytes="",
            data_uncompressed_bytes="",
            marks_bytes="",
            comment="",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=False,
        )

        columns = [column]
        oddrn_path = "tables"
        result = build_dataset_fields(columns, self.oddrn_generator, oddrn_path)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], DataSetField)
        self.assertEqual(result[0].parent_field_oddrn, None)
        self.assertEqual(result[0].type.type, Type.TYPE_STRING)
        self.assertEqual(result[0].type.logical_type, "String")

    def test_nested_case(self):
        column = Column(
            database="test",
            table="test",
            name="a.b",
            type="Array(String)",
            position=0,
            default_kind="",
            default_expression="",
            data_compressed_bytes="",
            data_uncompressed_bytes="",
            marks_bytes="",
            comment="",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=False,
        )

        column1 = Column(
            database="test",
            table="test",
            name="a.c",
            type="Array(Tuple(String, String))",
            position=1,
            default_kind="",
            default_expression="",
            data_compressed_bytes="",
            data_uncompressed_bytes="",
            marks_bytes="",
            comment="",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=False,
        )

        column2 = Column(
            database="test",
            table="test",
            name="d",
            type="Map(String, Array(UInt64))",
            position=2,
            default_kind="",
            default_expression="",
            data_compressed_bytes="",
            data_uncompressed_bytes="",
            marks_bytes="",
            comment="",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=False,
        )

        column3 = Column(
            database="test",
            table="test",
            name="e.f",
            type="Array(Nested(g String, h Nested(i Tuple(String, UInt64), j String)))",
            position=3,
            default_kind="",
            default_expression="",
            data_compressed_bytes="",
            data_uncompressed_bytes="",
            marks_bytes="",
            comment="",
            is_in_partition_key=False,
            is_in_sorting_key=False,
            is_in_primary_key=False,
            is_in_sampling_key=False,
            compression_codec=False,
        )

        columns = [column, column1, column2, column3]
        oddrn_path = "tables"
        result = build_dataset_fields(columns, self.oddrn_generator, oddrn_path)

        self.assertEqual(len(result), 16)

        for item in result:
            self.assertIsInstance(item, DataSetField)
        # a
        self.assertEqual(result[0].parent_field_oddrn, None)
        self.assertEqual(result[0].type.type, Type.TYPE_LIST)
        self.assertEqual(result[0].type.logical_type, "Array")

        # a.b
        self.assertEqual(
            result[1].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/a",
        )
        self.assertEqual(result[1].type.type, Type.TYPE_STRING)
        self.assertEqual(result[1].type.logical_type, "String")

        # a.c
        self.assertEqual(
            result[2].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/a",
        )
        self.assertEqual(result[2].type.type, Type.TYPE_STRUCT)
        self.assertEqual(result[2].type.logical_type, "Tuple(String, String)")

        # tuple a.c.0
        self.assertEqual(
            result[3].parent_field_oddrn,
            result[2].oddrn,
        )
        self.assertEqual(
            result[3].oddrn,
            result[2].oddrn + "/keys/0",
        )
        self.assertEqual(result[3].type.type, Type.TYPE_STRING)
        self.assertEqual(result[3].type.logical_type, "String")

        # tuple a.c.1
        self.assertEqual(
            result[4].parent_field_oddrn,
            result[2].oddrn,
        )
        self.assertEqual(
            result[4].oddrn,
            result[2].oddrn + "/keys/1",
        )
        self.assertEqual(result[4].type.type, Type.TYPE_STRING)
        self.assertEqual(result[4].type.logical_type, "String")

        # d
        self.assertEqual(result[5].parent_field_oddrn, None)
        self.assertEqual(result[5].type.type, Type.TYPE_MAP)
        self.assertEqual(result[5].type.logical_type, "Map(String, Array(UInt64))")

        # d: key
        self.assertEqual(
            result[6].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/d",
        )
        self.assertEqual(
            result[6].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/d/keys/Key",
        )
        self.assertEqual(result[6].type.type, Type.TYPE_STRING)
        self.assertEqual(result[6].type.logical_type, "String")

        # d: value
        self.assertEqual(
            result[7].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/d",
        )
        self.assertEqual(
            result[7].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/d/keys/Value",
        )
        self.assertEqual(result[7].type.type, Type.TYPE_LIST)
        self.assertEqual(result[7].type.logical_type, "Array(UInt64)")

        # e
        self.assertEqual(result[8].parent_field_oddrn, None)
        self.assertEqual(
            result[8].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e",
        )
        self.assertEqual(result[8].parent_field_oddrn, None)
        self.assertEqual(result[8].type.type, Type.TYPE_LIST)
        self.assertEqual(result[8].type.logical_type, "Array")

        # e.f
        self.assertEqual(
            result[9].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e",
        )
        self.assertEqual(
            result[9].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f",
        )
        self.assertEqual(result[9].type.type, Type.TYPE_STRUCT)
        self.assertEqual(
            result[9].type.logical_type,
            "Nested(g String, h Nested(i Tuple(String, UInt64), j String))",
        )

        # e.f.g
        self.assertEqual(
            result[10].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f",
        )
        self.assertEqual(
            result[10].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/g",
        )
        self.assertEqual(result[10].type.type, Type.TYPE_STRING)
        self.assertEqual(result[10].type.logical_type, "String")

        # e.f.h
        self.assertEqual(
            result[11].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f",
        )
        self.assertEqual(
            result[11].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h",
        )
        self.assertEqual(result[11].type.type, Type.TYPE_STRUCT)
        self.assertEqual(
            result[11].type.logical_type, "Nested(i Tuple(String, UInt64), j String)"
        )

        # e.f.h.i
        self.assertEqual(
            result[12].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h",
        )
        self.assertEqual(
            result[12].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h/keys/i",
        )
        self.assertEqual(result[12].type.type, Type.TYPE_STRUCT)
        self.assertEqual(result[12].type.logical_type, "Tuple(String, UInt64)")

        # tuple e.f.h.i.0
        self.assertEqual(
            result[13].parent_field_oddrn,
            result[12].oddrn,
        )
        self.assertEqual(
            result[13].oddrn,
            result[12].oddrn + "/keys/0",
        )
        self.assertEqual(result[13].type.type, Type.TYPE_STRING)
        self.assertEqual(result[13].type.logical_type, "String")

        # tuple e.f.h.i.1
        self.assertEqual(
            result[14].parent_field_oddrn,
            result[12].oddrn,
        )
        self.assertEqual(
            result[14].oddrn,
            result[12].oddrn + "/keys/1",
        )
        self.assertEqual(result[14].type.type, Type.TYPE_INTEGER)
        self.assertEqual(result[14].type.logical_type, "UInt64")

        # e.f.h.j
        self.assertEqual(
            result[15].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h",
        )
        self.assertEqual(
            result[15].oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h/keys/j",
        )
        self.assertEqual(result[15].type.type, Type.TYPE_STRING)
        self.assertEqual(result[15].type.logical_type, "String")
