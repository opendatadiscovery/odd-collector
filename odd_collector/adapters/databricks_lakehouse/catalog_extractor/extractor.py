from odd_collector.adapters.databricks_lakehouse.client import DatabricksRestClient
import logging
from time import sleep
from pandas import DataFrame
import ast


class DatabricksCatalogExtractorException(Exception):
    def __init__(self, message):
        super().__init__(message)


class CatalogExtractor:
    def __init__(self, drc: DatabricksRestClient):
        self.drc = drc
        self.logs_delimiter = "ttt"

    def extract_catalog(self, run_id: int) -> DataFrame:
        run_output = self.drc.jobs_api.client.get_run_output(run_id)
        logs: str = run_output["logs"]
        catalogs_output: str = logs.split(self.logs_delimiter)[1]
        catalogs_dict: dict = ast.literal_eval(catalogs_output)
        return DataFrame(catalogs_dict)

    def get_catalog(self, run_id: int, timeout: int = 60) -> DataFrame:
        run_response = self.drc.jobs_api.client.get_run(run_id)
        state = run_response["state"]
        life_cycle_state = state["life_cycle_state"]
        if life_cycle_state == "TERMINATED":
            if state["result_state"] == "SUCCESS":
                return self.extract_catalog(run_id)
            else:
                raise DatabricksCatalogExtractorException(state)
        else:
            if life_cycle_state in ["PENDING", "RUNNING"]:
                logging.info(state["state_message"])
                sleep(timeout)
                return self.get_catalog(run_id)
            else:
                raise DatabricksCatalogExtractorException(state)

    def run(self):
        if "azuredatabricks.net" in self.drc.api_client.url:
            cluster_config_file_name = "azure_cluster.json"
        elif "cloud.databricks.com" in self.drc.api_client.url:
            cluster_config_file_name = "aws_cluster.json"
        elif "gcp.databricks.com" in self.drc.api_client.url:
            cluster_config_file_name = "gcp_cluster.json"
        else:
            raise NotImplementedError
        root_path = "odd_collector/adapters/databricks_lakehouse/catalog_extractor/"
        local_path = root_path + "odd_job"
        dbfs_path = "dbfs:/odd_job.py"
        run_name = "odd"
        job_timeout = 600

        self.drc.put_file(local_path, dbfs_path)
        run_id = self.drc.submit_run_and_get_id(
            dbfs_path, root_path + cluster_config_file_name, job_timeout, run_name
        )
        return self.get_catalog(run_id)
