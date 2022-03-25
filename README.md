# odd-collector
Aggregation of adapters for [ODD Platform](https://github.com/opendatadiscovery/odd-platform)

`odd-collector` uses [odd-collector-sdk](https://github.com/opendatadiscovery/odd-collector-sdk).


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
Due to Plugin is inherited from `pydantic.BaseSetting`, each field missed in `collector-config.yaml` can be taken from env variables.

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

## Building
```bash
docker build .
```

## Example of docker-compose.yaml
Custom `.env` file for docker-compose.yaml
```
PLATFORM_HOST_URL=http://odd-platform:8080
```

Custom `collector-config.yaml`
```yaml
provider_oddrn: collector
platform_host_url: "http://localhost:8080"
default_pulling_interval: 10
token: ""
plugins:
  - type: postgresql
    name: test_postgresql_adapter
    host: "localhost"
    port: 5432
    database: "some_database_name"
    user: "some_user_name"
    password: "some_password"
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
    depends_on:
      - odd-platform
```
