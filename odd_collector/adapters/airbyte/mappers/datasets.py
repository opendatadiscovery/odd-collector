import oddrn_generator
from typing import Optional
from re import search
from ..logger import logger


def get_dataset_generator(
    name: str, host_settings: str, database: str
) -> Optional[oddrn_generator.Generator]:
    fabric = {
        "postgres": oddrn_generator.PostgresqlGenerator,
        "mysql": oddrn_generator.MysqlGenerator,
        "mssql": oddrn_generator.MssqlGenerator,
        "clickhouse": oddrn_generator.ClickHouseGenerator,
        "redshift": oddrn_generator.RedshiftGenerator,
        "mongodb": oddrn_generator.MongoGenerator,
        "kafka": oddrn_generator.KafkaGenerator,
        "snowflake": oddrn_generator.SnowflakeGenerator,
        "elasticsearch": oddrn_generator.ElasticSearchGenerator,
        "metabase": oddrn_generator.MetabaseGenerator,
        "oracle": oddrn_generator.OracleGenerator,
    }
    try:
        generator = fabric[name](host_settings=host_settings, databases=database)
        return generator
    except KeyError:
        logger.warning(f"Generator not available for dataset: {name}")
        return None


def generate_dataset_oddrn(is_source: bool, dataset_meta: dict) -> Optional[str]:
    """
    Function intended for generating oddrns of
    sources and destinations in Airbyte connections
    """

    name = (
        str(dataset_meta.get("sourceName")).lower()
        if is_source
        else str(dataset_meta.get("destinationName")).lower()
    )
    conn_conf = dataset_meta.get("connectionConfiguration")

    if "host" in conn_conf.keys():
        host_settings = conn_conf.get("host")
    elif "host" in conn_conf.get("instance_type").keys():
        host_settings = conn_conf.get("instance_type").get("host")
    else:
        logger.warning("Couldn't find host setting in the connection metadata")

    database = conn_conf.get("database")
    database = database.upper() if name == "snowflake" else database

    oddrn_gen = get_dataset_generator(name, host_settings, database)
    if oddrn_gen:
        oddrn = oddrn_gen.get_data_source_oddrn()
        return oddrn


def filter_dataset_oddrn(
    connection_meta: dict, dataset_oddrns: list[str]
) -> list[Optional[str]]:
    """
    Function to filter only replicated data sources
    """
    replicated_tables = []
    for stream in connection_meta["syncCatalog"]["streams"]:
        replicated_tables.append(stream["stream"]["name"].lower())

    entities = []

    for oddrn in dataset_oddrns:
        # search for table names in oddrns
        res1 = (
            search(r"\w+$", oddrn).group().lower() if search(r"\w+$", oddrn) else None
        )
        # search for _airbyte_raw tables in oddrns
        res2 = (
            search(r"_([^_]+$)", oddrn).group(1).lower()
            if search(r"_([^_]+$)", oddrn)
            else None
        )
        if res1 in replicated_tables or res2 in replicated_tables:
            entities.append(oddrn)
    return entities
