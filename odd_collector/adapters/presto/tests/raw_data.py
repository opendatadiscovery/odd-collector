tables_nodes = [
    ["mongodb", "config", "system.sessions", "TABLE"],
    ["mongodb", "local", "startup_log", "TABLE"],
    ["mysql", "test_schema_mysql", "test_table_mysql", "TABLE"],
    ["mysql", "test_schema_mysql", "test_table_mysql2", "TABLE"],
    ["mysql", "test_schema_mysql2", "test_table_mysql", "TABLE"],
    ["postgresql", "test_schema_postgres", "test_table_postgres", "TABLE"],
    ["redis", "schema1", "table1", "TABLE"],
    ["redis", "schema1", "table2", "TABLE"],
]

columns_nodes = [
    ["mongodb", "local", "startup_log", "_id", "varchar"],
    ["mongodb", "local", "startup_log", "hostname", "varchar"],
    ["mongodb", "local", "startup_log", "starttime", "timestamp"],
    ["mongodb", "local", "startup_log", "starttimelocal", "varchar"],
    ["mongodb", "local", "startup_log", "cmdline", 'row("net" row("bindIp" varchar))'],
    ["mongodb", "local", "startup_log", "pid", "bigint"],
    ["mysql", "test_schema_mysql", "test_table_mysql", "id", "integer"],
    ["mysql", "test_schema_mysql", "test_table_mysql", "value", "varchar(10)"],
    ["mysql", "test_schema_mysql", "test_table_mysql2", "id", "integer"],
    ["mysql", "test_schema_mysql", "test_table_mysql2", "value", "varchar(10)"],
    ["mysql", "test_schema_mysql2", "test_table_mysql", "id", "integer"],
    ["mysql", "test_schema_mysql2", "test_table_mysql", "value", "varchar(10)"],
    ["postgresql", "test_schema_postgres", "test_table_postgres", "id", "integer"],
    [
        "postgresql",
        "test_schema_postgres",
        "test_table_postgres",
        "value",
        "varchar(10)",
    ],
]

nested_nodes = {
    "mongodb": {
        "local": {
            "startup_log": [
                {"column_name": "_id", "type_name": "varchar"},
                {"column_name": "hostname", "type_name": "varchar"},
                {"column_name": "starttime", "type_name": "timestamp"},
                {"column_name": "starttimelocal", "type_name": "varchar"},
                {
                    "column_name": "cmdline",
                    "type_name": 'row("net" row("bindIp" varchar))',
                },
                {"column_name": "pid", "type_name": "bigint"},
            ]
        }
    },
    "mysql": {
        "test_schema_mysql": {
            "test_table_mysql": [
                {"column_name": "id", "type_name": "integer"},
                {"column_name": "value", "type_name": "varchar(10)"},
            ],
            "test_table_mysql2": [
                {"column_name": "id", "type_name": "integer"},
                {"column_name": "value", "type_name": "varchar(10)"},
            ],
        },
        "test_schema_mysql2": {
            "test_table_mysql": [
                {"column_name": "id", "type_name": "integer"},
                {"column_name": "value", "type_name": "varchar(10)"},
            ]
        },
    },
    "postgresql": {
        "test_schema_postgres": {
            "test_table_postgres": [
                {"column_name": "id", "type_name": "integer"},
                {"column_name": "value", "type_name": "varchar(10)"},
            ]
        }
    },
}
pass
