[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
# odd-collector
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Preview:
- [Implemented adapters](#implemented-adapters)
- [How to build](#building)
- [M1 Building issue](#m1-building)
- [Config example](#config-example)

## Implemented adapters
 - [Cassandra](#cassandra)
 - [ClickHouse](#clickhouse)
 - [Dbt](#dbt)
 - [Elasticsearch](#elasticsearch) 
 - [Feast](#feast)
 - [Hive](#hive)
 - [Kubeflow](#kubeflow)
 - [MongoDb](#mongodb)
 - [MySQL](#mysql)
 - [Neo4j](#neo4j)
 - [PostgreSQL](#postgresql) 
 - [Redshift](#redshift) 
 - [Snowflake](#snowflake)
 - [Tarantool](#tarantool)
 - [Tableau](#tableau)

### __Cassandra__
```yaml
type: cassandra
name: cassandra
host: str
port: int
database: str
user: str
password: str
contact_points: str[]
```

### __ClickHouse__
```yaml
type: clickhouse
name: clickhouse
host: str
port: int
database: str
user: str
password: str
```

### __Dbt__
```yaml
type: dbt
name: dbt
host: str
odd_catalog_url: str
```

### __Elasticsearch__
```yaml
type: elasticsearch
name: elasticsearch
host: str
port: int
database: ""
user: ""
password: ""
```

### __Feast__
```yaml
type: feast
name: feast
host: str
port: int
database: ""
user: ""
password: ""
repo_path: str
```

### __Hive__
```yaml
type: hive
name: hive
host: str
port: int
database: str
user: str
password: str
```

### __Kubeflow__
```yaml
type: kubeflow
name: kubeflow
host: str
namespace: str
session_cookie0: Optional[str]
session_cookie1: Optional[str]
```
### __MongoDB__
```yaml
type: mongodb
name: mongodb
host: str
port: int
database: str
user: str
password: str
protocol: str
```
### __MySQL__
```yaml
type: mysql
name: mysql
host: str
port: int
database: str
user: str
password: str
ssl_disabled: bool
```
### __Neo4j__
```yaml
type: neo4j
host: str
port: int
database: str
user: str
password: str
```
### __PostgreSQL__
```yaml
type: postgresql
name: postgresql
host: str
port: int
database: str
user: str
password: str
```
### __Redshift__
```yaml
type: redshift
name: redshift
host: str
port: int
database: str
user: str
password: str
```
### __Snowflake__
```yaml
type: snowflake
name: snowflake
host: str
port: int
database: str
user: str
password: str
account: str
warehouse: str
```
### __Tableau__
```yaml
type: tableau
name: tableau
server: str
site: str
user: str
password: str
```
### __Tarantool__
```yaml
type: tarantool
name: tarantool
host: str
port: int
user: ""
password: ""
```

## Building
```bash
docker build .
```

## M1 building issue

**libraries** `pyodbc` , `confluent-kafka` and `grpcio`   have problem during installing and building project on Mac M1.

- https://github.com/mkleehammer/pyodbc/issues/846
- https://github.com/confluentinc/confluent-kafka-python/issues/1190
- https://github.com/grpc/grpc/issues/25082

Possible solutions

```bash
# NOTE: be aware of versions
# NOTE: easiest way is to add all export statements to your .bashrc/.zshrc file

# pyodbc dependencies
brew install unixodbc freetds openssl

export LDFLAGS="-L/opt/homebrew/lib  -L/opt/homebrew/Cellar/unixodbc/2.3.11/include -L/opt/homebrew/opt/freetds/lib -L/opt/homebrew/opt/openssl@3/lib"
export CFLAGS="-I/opt/homebrew/Cellar/unixodbc/2.3.11/include -I/opt/homebrew/opt/freetds/include"
export CPPFLAGS="-I/opt/homebrew/include -I/opt/homebrew/Cellar/unixodbc/2.3.11/include -I/opt/homebrew/opt/openssl@3/include"

# cunfluent-kafka
brew install librdkafka

export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/include
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/1.9.0/lib
export PATH="/opt/homebrew/opt/openssl@3/bin:$PATH"

# grpcio
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
```

## Config example
Custom `.env` file for docker-compose.yaml
```
PLATFORM_HOST_URL=http://odd-platform:8080
POSTGRES_PASSWORD=postgres_password_secret
```

There are 3 options for config field pass:
1. Explicitly set it in `collector_config.yaml` file, i.e `database: odd-platform-db`
2. Use `.env` file, Pydantic will read skipped field from ENV variable
3. In situation when plugins have same field names, we can  explicitly set ENV variable to `collector_config.yaml`, i.e. `password: !ENV ${POSTGRES_PASSWORD}`

Custom `collector-config.yaml`
```yaml
# platform_host_url: "http://localhost:8080" - We can skip it, it will be taken by pydantic from ENV variables
default_pulling_interval: 10
token: ""
plugins:
  - type: postgresql
    name: test_postgresql_adapter
    host: "localhost"
    port: 5432
    database: "some_database_name"
    user: "some_user_name"
    password: !ENV ${POSTGRES_PASSWORD}
  - type: mysql
    name: test_mysql_adapter
    host: "localhost"
    port: 3306
    database: "some_database_name"
    user: "some_user_name"
    password: "some_password"
```

docker-compose.yaml
```yaml
version: "3.8"
services:
  # --- ODD Platform ---
  database:
    ...
  odd-platform:
    ...
  
  odd-collector:
    image: 'image_name'
    restart: always
    volumes:
      - collector_config.yaml:/app/collector_config.yaml
    environment:
      - PLATFORM_HOST_URL=${PLATFORM_HOST_URL}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
    depends_on:
      - odd-platform
```
