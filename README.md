# odd-collector
ODD Collector is a lightweight service which gathers metadata from all your data sources.

To learn more about collector types and ODD Platform's architecture, [read the documentation](https://docs.opendatadiscovery.org/architecture).

## Domain
___
Main class for plugins is `Plugins` inherited from `odd-collector-sdk.domain.Plugin`. 
```python
class Plugins(Plugin):
    host: str
    port: int
    database: str
    user: str
    password: str
```

## Implemented adapters
___
### __PostgreSQL__
```yaml
type: postgresql
host: str
port: int
database: str
user: str
password: str
```
### __MySQL__
```yaml
type: mysql
host: str
port: int
database: str
user: str
password: str
ssl_disabled: bool
```
### __ClickHouse__
```yaml
type: clickhouse
host: str
port: int
database: str
user: str
password: str
```
### __Redshift__
```yaml
type: redshift
host: str
port: int
database: str
user: str
password: str
```
### __Hive__
```yaml
type: hive
host: str
port: int
database: str
user: str
password: str
```
### __Elasticsearch__
```yaml
type: elasticsearch
host: str
port: int
database: ""
user: ""
password: ""
```

## Building
```bash
docker build .
```

## Example of docker-compose.yaml
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
    password: !ENV {POSTGRES_PASSWORD}
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
