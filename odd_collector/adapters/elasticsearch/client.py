from typing import Optional

from elasticsearch import Elasticsearch

from odd_collector.domain.plugin import ElasticsearchPlugin


class Client:
    def __init__(self, config: ElasticsearchPlugin):
        self._es = Elasticsearch(
            hosts=[f"{config.host}:{config.port}"],
            basic_auth=(config.username, config.password.get_secret_value()),
            verify_certs=config.verify_certs,
            ca_certs=config.ca_certs,
        )

    def get_indices(self, index: Optional[str] = None, h=None) -> list:
        return self._es.cat.indices(format="json", index=index, h=h).body

    def get_mapping(self, index_name: Optional[str] = None) -> dict:
        return self._es.indices.get_mapping(index=index_name).body

    def get_index_settings(self, index_name: str) -> dict:
        return self._es.indices.get_settings(index=index_name).body

    def get_data_streams(self, name: Optional[str] = None) -> dict:
        response = self._es.indices.get_data_stream(name=name)
        return response["data_streams"]

    def get_index_template(self, template_name: str) -> list[dict]:
        return self._es.indices.get_index_template(name=template_name).body.get(
            "index_templates"
        )
