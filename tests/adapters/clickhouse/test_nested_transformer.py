import unittest

from odd_collector.adapters.clickhouse.domain import Column, NestedColumn
from odd_collector.adapters.clickhouse.mappers.columns import build_nested_columns


class TestColumnTransformator(unittest.TestCase):
    def test_transform_simple_column(self):
        column = Column(
            database="test_database",
            table="test_table",
            name="test",
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
        column1 = Column(
            database="test_database",
            table="test_table",
            name="test1",
            type="UInt64",
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
            database="test_database",
            table="test_table",
            name="test2",
            type="String",
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
        columns = [column, column1, column2]

        expected_column = NestedColumn.from_column(column, items=[])
        expected_column1 = NestedColumn.from_column(column1, items=[])
        expected_column2 = NestedColumn.from_column(column2, items=[])
        expected_result = [expected_column, expected_column1, expected_column2]

        result = build_nested_columns(columns)
        self.assertEqual(len(result), len(expected_result))

        for i in range(len(result)):
            expected_column = expected_result[i]
            column = result[i]
            self.assertIsInstance(expected_column, NestedColumn)
            self.assertEqual(expected_column.database, column.database)
            self.assertEqual(expected_column.name, column.name)
            self.assertEqual(expected_column.table, column.table)
            self.assertEqual(expected_column.type, column.type)
            self.assertEqual(expected_column.items, column.items)

    def test_nested_columns(self):
        column = Column(
            database="test_database",
            table="test_table",
            name="test.key",
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
            database="test_database",
            table="another_test_table",
            name="test1",
            type="UInt32",
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
        columns = [column, column1]
        expected_column = NestedColumn.from_column(
            column=column,
            new_name="test",
            new_type="Nested",
            items=[
                NestedColumn.from_column(
                    column=column, new_name="key", new_type="String", items=[]
                )
            ],
        )
        expected_column1 = NestedColumn.from_column(column1, items=[])
        result = build_nested_columns(columns)

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].items), 1)
        self.assertEqual(result[0].name, expected_column.name)
        self.assertEqual(result[0].type, expected_column.type)
        self.assertEqual(result[0].items[0].name, expected_column.items[0].name)
        self.assertEqual(result[0].items[0].type, expected_column.items[0].type)
        self.assertEqual(result[0].table, "test_table")

        self.assertEqual(result[1].table, "another_test_table")
        self.assertEqual(result[1].name, expected_column1.name)
        self.assertEqual(result[1].type, expected_column1.type)
        self.assertEqual(result[1].items, expected_column1.items)

    def test_complex_nested_column(self):
        column = Column(
            database="test_database",
            table="test_table",
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
            database="test_database",
            table="test_table",
            name="a.c",
            type="Array(String)",
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
            database="test_database",
            table="test_table",
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
            database="test_database",
            table="test_table",
            name="e.q",
            type="Array(Nested(f String, k Nested(l UInt64, m String)))",
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

        result = build_nested_columns(columns)

        self.assertEqual(len(result), 3)

        self.assertIsInstance(result[0], NestedColumn)
        self.assertIsInstance(result[1], NestedColumn)
        self.assertIsInstance(result[2], NestedColumn)

        self.assertEqual(result[0].name, "a")
        self.assertEqual(len(result[0].items), 2)
        self.assertEqual(result[0].items[0].name, "b")
        self.assertEqual(result[0].items[0].type, "String")

        self.assertEqual(result[0].items[1].name, "c")
        self.assertEqual(result[0].items[1].type, "String")
        self.assertEqual(len(result[0].items[1].items), 0)

        self.assertEqual(result[1].name, "d")
        self.assertEqual(result[1].type, "Map")
        self.assertEqual(len(result[1].items), 0)

        self.assertEqual(result[2].name, "e")
        self.assertEqual(len(result[2].items), 1)
        self.assertEqual(result[2].items[0].name, "q")
        self.assertEqual(len(result[2].items[0].items), 2)
        self.assertEqual(result[2].items[0].items[0].name, "f")
        self.assertEqual(result[2].items[0].items[1].name, "k")
        self.assertEqual(result[2].items[0].items[0].type, "String")

        self.assertEqual(len(result[2].items[0].items[1].items), 2)
        self.assertEqual(result[2].items[0].items[1].items[0].name, "l")
        self.assertEqual(result[2].items[0].items[1].items[1].name, "m")

    def test_type_shouldnt_processed(self):
        column = Column(
            database="test_database",
            table="test_table",
            name="a",
            type="Array(Nested(b String))",
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
        result = build_nested_columns(columns)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], NestedColumn)
        self.assertEqual(result[0].name, "a")
        # This is nested type, this is array. Column name should have '.' if type is Nested
        self.assertEqual(result[0].type, "Array(Nested(b String))")

    def test_simple_tuple(self):
        column = Column(
            database="test_database",
            table="test_table",
            name="a.b",
            type="Array(Nested(c Tuple(String, UInt32)))",
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

        result = build_nested_columns(columns)

        self.assertEqual(len(result), 1)
        self.assertIsInstance(result[0], NestedColumn)
        self.assertEqual(result[0].name, "a")
        self.assertEqual(len(result[0].items), 1)

        self.assertIsInstance(result[0].items[0], NestedColumn)
        self.assertEqual(result[0].items[0].name, "b")
        # TODO: add check that type of 'b' items in tuple
