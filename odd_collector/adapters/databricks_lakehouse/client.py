from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.jobs.api import JobsApi
from odd_collector.domain.plugin import DatabricksPlugin
from json import load
from urllib.parse import urlparse


class DatabricksRestClient:
    def __init__(self, config: DatabricksPlugin):
        self.__host = f"https://{config.workspace}"
        self.api_client = ApiClient(
            host=self.__host,
            token=config.token
        )

    def get_server_host(self) -> str:
        return urlparse(self.__host).netloc

    def put_file(self, local_path: str, dbfs_path: str, overwrite: bool = True):
        dbfs_api = DbfsApi(self.api_client)
        dbfs_api.put_file(local_path, DbfsPath(dbfs_path), overwrite)

    @property
    def jobs_api(self):
        return JobsApi(self.api_client)

    @staticmethod
    def __get_cluster_config_from_file(cluster_config_local_json_path: str) -> dict:
        with open(cluster_config_local_json_path, 'rb') as file:
            return load(file)

    def submit_run_and_get_id(self,
                              dbfs_script_path: str, cluster_config_local_json_path: str,
                              run_name=None) -> int:
        resp = self.jobs_api.client.submit_run(run_name=run_name,
                                               new_cluster=self.__get_cluster_config_from_file(
                                                   cluster_config_local_json_path),
                                               spark_python_task={
                                                   "python_file": dbfs_script_path,
                                               })
        return resp['run_id']
