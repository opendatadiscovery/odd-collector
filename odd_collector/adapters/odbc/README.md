# ODBC/MS SQL adapter

### Set up development/testing environment

Assuming you have `odd-platform` pulled to a sibling directory to the `odd-collector`:

0. Configure `collector_config.yaml`:
```yaml
  - type: odbc
    name: my_odbc
    host: sample_mssql
    port: 1433
    database: msdb
    driver: "{ODBC Driver 17 for SQL Server}"
    user: "sa"
    password: "Password0"
```
(`msdb` database is the one present in MS SQL by default)
1. Copy `docker/testodbc.yaml` to `../odd-platform/docker`
2. Start the ODD platform:
```shell
cd ../odd-platform/docker
docker-compose -f testodbc.yaml build
docker-compose -f testodbc.yaml up -d
```

3. Then either
```shell
docker-compose -f testodbc.yaml up -d
```
or start that compose file from your IDE.

This will bring up the ODD platform, the database for it, the ODD collector and the sample MS SQL container (it will have user `sa` with password specified in `SA_PASSWORD` environment variable in `testodbc.yaml` compose file. (Note: the password in MS SQL is subject to some 'strength' rules)

4. Create a collector in `odd-platform` UI ( http://localhost:8080 ), copy the token and put it into `collector_config.yaml`.

### Development/testing cycle
Assuming you started docker-compose file like described in a previous section, you can work on testing/development as follows:

The `collector_config.yaml` is available to `odd-collector` container as a volume, so you can change it on the fly and then restart `odd-collector` container (but not the rest of environment brought up as compose file) to have `collector_config.yaml` re-read and data source metadata sent to ODD platform.

Once successful, you should have a new data source `my_odbc` listed on ODD platform UI in http://localhost:8080/management/datasources and a bunch of its new tables
in http://localhost:8080/search

If you've changed the code of an adapter, then to have it make effect, you will have to:
```shell
cd ../odd-platform/docker
docker-compose -f testodbc.yaml up -d --build odd-collector
```
(If you restarted whole compose file, then generate a new token, stick it to `collector_config.yaml` and restart `odd-collector`)

### Using non-SA MS SQL user for testing/development
If you want to, you'll have to create 'normal' user in MS SQL.
To do so, run on `sample_mssql` container:
```shell
/opt/mssql-tools/bin/sqlcmd -U sa 
```

And execute following in an interactive shell:
```sql
USE [master]
create login testlogin with password=‘Password0’
create user test for login testlogin
ALTER LOGIN [testlogin] WITH DEFAULT_DATABASE=[master]
GRANT CONNECT SQL TO [testlogin]
ALTER LOGIN [testlogin] ENABLE
GO
```
You can also automate this if you wish :)