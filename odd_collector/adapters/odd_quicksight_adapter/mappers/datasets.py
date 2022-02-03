from typing import Dict, Any

from more_itertools import flatten
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer

from . import metadata_extractor
from .columns import map_column
from oddrn_generator import QuicksightGenerator, AthenaGenerator, PostgresqlGenerator, MysqlGenerator, \
    SnowflakeGenerator


def map_quicksight_dataset(raw_dataset_data: Dict[str, Any],
                           account_id: str,
                           region_name: str,
                           quicksight_client) -> DataEntity:
    oddrn_gen = QuicksightGenerator(
        cloud_settings={'account': account_id, 'region': region_name},
        datasets=raw_dataset_data['DataSetId']
    )

    columns = flatten([
        map_column(rc, oddrn_gen.get_oddrn_by_path("datasets"))
        for rc in raw_dataset_data['OutputColumns']
    ])

    raw_dataset_data['DataTransformer'] = []
    for k in raw_dataset_data['PhysicalTableMap']:
        for j in raw_dataset_data['PhysicalTableMap'][k]:
            source_id = raw_dataset_data['PhysicalTableMap'][k][j]['DataSourceArn'].split('/')[-1]
            source_info = __describe_data_source(quicksight_client, source_id, account_id)
            source_type = source_info['Type']
            oddrn = ''
            if source_type == 'S3':
                source_bucket = source_info['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Bucket']
                source_key = source_info['DataSourceParameters']['S3Parameters']['ManifestFileLocation']['Key']
                # TODO: Generate s3 oddrn using generator lib
                oddrn = f'//s3/cloud/aws' \
                        f'/account/{account_id}' \
                        f'/region/{region_name}' \
                        f'/bucket/{source_bucket}' \
                        f'/key/{source_key}'

            elif source_type == 'ATHENA':
                athena_oddrn = AthenaGenerator(
                    cloud_settings={'account': account_id, 'region': region_name},
                    catalogs=raw_dataset_data['PhysicalTableMap'][k]['RelationalTable']['Catalog'],
                    databases=raw_dataset_data['PhysicalTableMap'][k]['RelationalTable']['Schema'],
                    tables=raw_dataset_data['PhysicalTableMap'][k]['RelationalTable']['Name']
                )
                oddrn = athena_oddrn.get_oddrn_by_path("tables")

            elif source_type == 'POSTGRESQL':
                host = source_info['DataSourceParameters']['PostgreSqlParameters']['Host']
                port = source_info['DataSourceParameters']['PostgreSqlParameters']['Port']
                database = source_info['DataSourceParameters']['PostgreSqlParameters']['Database']

                postgres_oddrn = PostgresqlGenerator(
                    host_settings=f'{host}:{port}',
                    databases=database
                )
                oddrn = postgres_oddrn.get_oddrn_by_path("databases")

            elif source_type == 'MYSQL':
                host = source_info['DataSourceParameters']['MySqlParameters']['Host']
                port = source_info['DataSourceParameters']['MySqlParameters']['Port']
                database = source_info['DataSourceParameters']['MySqlParameters']['Database']

                mysql_oddrn = MysqlGenerator(
                    host_settings=f'{host}:{port}',
                    databases=database
                )
                oddrn = mysql_oddrn.get_oddrn_by_path("databases")

            elif source_type == 'SNOWFLAKE':
                host = source_info['DataSourceParameters']['SnowflakeParameters']['Host']
                database = source_info['DataSourceParameters']['SnowflakeParameters']['Database']
                warehouse = source_info['DataSourceParameters']['SnowflakeParameters']['Warehouse']

                snowflake_oddrn = SnowflakeGenerator(
                    host_settings=host,
                    warehouses=warehouse,
                    databases=database
                )
                oddrn = snowflake_oddrn.get_oddrn_by_path("databases")

            raw_dataset_data['DataTransformer'].append({
                'oddrn': oddrn,
                'metadata': source_info
            })
    return DataEntity(
        oddrn=oddrn_gen.get_oddrn_by_path("datasets"),
        name=raw_dataset_data['Name'],
        type=DataEntityType.TABLE,
        created_at=raw_dataset_data['CreatedTime'].isoformat(),
        updated_at=raw_dataset_data['LastUpdatedTime'],
        metadata=[metadata_extractor.extract_dataset_metadata(raw_dataset_data)],
        dataset=DataSet(
            parent_oddrn=None,
            rows_number=0,
            field_list=list(columns)
        ),
        data_transformer=DataTransformer(
            inputs=[transformer['oddrn'] for transformer in raw_dataset_data['DataTransformer']],
            outputs=[]
        )
    )


def __describe_data_source(client, data_source_id: str, account_id: str):
    raw_data_sources: Dict[str, Any] = \
        client.describe_data_source(
            AwsAccountId=account_id,
            DataSourceId=data_source_id
        )['DataSource']
    return raw_data_sources
