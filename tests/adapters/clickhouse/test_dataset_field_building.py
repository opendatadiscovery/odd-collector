import unittest

from oddrn_generator import ClickHouseGenerator
from odd_collector.adapters.clickhouse.mappers.columns import build_nested_columns, to_dataset_fields
from odd_models.models import DataSetField, Type
from odd_collector.adapters.clickhouse.domain import Column, NestedColumn
from odd_collector.adapters.clickhouse.mappers.tables import transformer


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
        nested_columns = build_nested_columns(columns)
        oddrn_path = "tables"
        result = to_dataset_fields(
            self.oddrn_generator, oddrn_path, nested_columns
        )

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
            type="Map",
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
        nested_columns = build_nested_columns(columns)
        oddrn_path = "tables"
        result = to_dataset_fields(
            self.oddrn_generator, oddrn_path, nested_columns
        )

        self.assertEqual(len(result), 10)

        for item in result:
            self.assertIsInstance(item, DataSetField)

        self.assertEqual(result[0].parent_field_oddrn, None)
        self.assertEqual(result[0].type.type, Type.TYPE_LIST)
        self.assertEqual(result[0].type.logical_type, "Nested")

        self.assertEqual(
            result[1].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/a",
        )
        self.assertEqual(result[1].type.type, Type.TYPE_STRING)
        self.assertEqual(result[1].type.logical_type, "String")

        self.assertEqual(
            result[2].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/a",
        )
        # TODO: Change to Tuple type
        self.assertEqual(result[2].type.type, Type.TYPE_STRUCT)
        self.assertEqual(result[2].type.logical_type, "Tuple(String, String)")

        self.assertEqual(result[3].parent_field_oddrn, None)
        self.assertEqual(result[3].type.type, Type.TYPE_MAP)
        self.assertEqual(result[3].type.logical_type, "Map")

        self.assertEqual(result[4].parent_field_oddrn, None)
        self.assertEqual(result[4].type.type, Type.TYPE_LIST)
        self.assertEqual(result[4].type.logical_type, "Nested")

        self.assertEqual(
            result[5].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e",
        )
        self.assertEqual(result[5].type.type, Type.TYPE_LIST)
        self.assertEqual(
            result[5].type.logical_type,
            "Nested(g String, h Nested(i Tuple(String, UInt64), j String))",
        )

        self.assertEqual(
            result[6].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f",
        )
        self.assertEqual(result[6].type.type, Type.TYPE_STRING)
        self.assertEqual(result[6].type.logical_type, "String")

        self.assertEqual(
            result[7].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f",
        )
        self.assertEqual(result[7].type.type, Type.TYPE_LIST)
        self.assertEqual(
            result[7].type.logical_type, "Nested(i Tuple(String, UInt64), j String)"
        )

        self.assertEqual(
            result[8].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h",
        )
        # TODO: Change type to Tuple
        self.assertEqual(result[8].type.type, Type.TYPE_STRUCT)
        self.assertEqual(result[8].type.logical_type, "Tuple(String, UInt64)")

        self.assertEqual(
            result[9].parent_field_oddrn,
            "//clickhouse/host/test/databases/test/tables/test/columns/e/keys/f/keys/h",
        )
        # TODO: Change type to Tuple
        self.assertEqual(result[9].type.type, Type.TYPE_STRING)
        self.assertEqual(result[9].type.logical_type, "String")
