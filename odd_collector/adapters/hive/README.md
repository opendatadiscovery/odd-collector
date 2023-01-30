## Developing notes

How to test hive adapter

#### Run hive with metastore.

Run it locally or clone and run [docker-hive](https://github.com/big-data-europe/docker-hive)

**NOTE:** It can have an issue running on M1. Use Rosetta or [Colima](https://github.com/abiosoft/colima) to avoid it.

#### Connect to Hive container

```commandline
docker-compose exec hive-server bash
```

#### Connect to Hive

```commandline
/opt/hive/bin/beeline -u jdbc:hive2://
```

### Create simple table

```commandline
CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
```

### Complex tables

Create helper table for the next usage

```text
DROP TABLE IF EXISTS one_row;
CREATE TABLE one_row (number_of_rows INT);
INSERT INTO TABLE one_row VALUES (1);
```

#### Table with primitive and complex types

```text
CREATE TABLE one_row_complex (
    `boolean` BOOLEAN,
    `tinyint` TINYINT,
    `smallint` SMALLINT,
    `int` INT,
    `bigint` BIGINT,
    `float` FLOAT,
    `double` DOUBLE,
    `string` STRING,
    `timestamp` TIMESTAMP,
    `binary` BINARY,
    `array` ARRAY<int>,
    `map` MAP<int, int>,
    `struct` STRUCT<a: int, b: int>,
    `decimal` DECIMAL(10, 1)
    );
INSERT OVERWRITE TABLE one_row_complex SELECT
    true,
    127,
    32767,
    2147483647,
    9223372036854775807,
    0.5,
    0.25,
    'a string',
    0,
    '123',
    array(1, 2),
    map(1, 2, 3, 4),
    named_struct('a', 1, 'b', 2),
    0.1
FROM one_row;
```

#### Table with nested complex datatypes

```text
CREATE TABLE one_row_nested_complex_types (
    `array` ARRAY<int>,
    `map` MAP<int, MAP<int, int>>,
    `struct` STRUCT<a: int, b: MAP<int, int>>
);
```

#### Simple view depending on one table

```text
CREATE VIEW one_row_complex_view AS
SELECT * FROM one_row_complex;
```

#### Simple view depending on view and table

```text
set hive.strict.checks.cartesian.product = false
```

```text
CREATE VIEW one_row_complex_view_2 AS
SELECT t.`boolean`, v.`tinyint` FROM one_row_complex as t, one_row_complex_view as v;
```