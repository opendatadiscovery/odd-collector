from odd_models.models import DataConsumer, DataEntity, DataEntityType

from ..domain.report import Report
from ..generator import ModeGenerator
from .metadata import map_report_metadata

# list of returned values of adapter:
# https://mode.com/developer/api-reference/management/data-sources/
DRIVER_MAPPING = {
    "jdbc:athena": "athena",
    "jdbc:bigquery": "bigquery",
    "jdbc:databricks": "databricks",
    "jdbc:transform": "transform",
    "jdbc:druid": "druid",
    "jdbc:hive": "hive",
    "jdbc:impala": "impala",
    "jdbc:mysql": "mysql",
    "jdbc:oracle": "oracle",
    "jdbc:oracleadb": "oracle",
    "jdbc:postgresql": "postgresql",
    "jdbc:presto": "presto",
    "jdbc:redshift": "redshift",
    "jdbc:snowflake": "snowflake",
    "jdbc:spark": "spark",
    "jdbc:sqlserver": "mssql",
    "jdbc:treasuredata": "treasuredata",
    "jdbc:vertica": "vertica",
}


def map_report(oddrn_generator: ModeGenerator, report: Report) -> DataEntity:
    inputs = set()
    for source in report.get_report_db_sources():
        host = source.server if source.server else report.host
        database = source.database if source.database else report.database
        driver = DRIVER_MAPPING[report.adapter]
        inputs.add(
            f"//{driver}/host/{host}/databases/{database}/schemas/{source.schema}/tables/{source.table}"
        )

    entity = DataEntity(
        oddrn=report.get_oddrn(oddrn_generator),
        name=report.name,
        metadata=[map_report_metadata(report)],
        tags=None,
        type=DataEntityType.DASHBOARD,
        updated_at=report.updated_at,
        created_at=report.created_at,
        data_consumer=DataConsumer(inputs=inputs),
    )
    return entity
