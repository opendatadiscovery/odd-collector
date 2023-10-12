from typing import Optional

from opensearchpy import OpenSearch

from odd_collector.domain.plugin import OpensearchPlugin


class Client:
    def __init__(self, config: OpensearchPlugin):
        self._os = OpenSearch(
            hosts=[f"{config.host}:{config.port}"],
            basic_auth=(config.username, config.password.get_secret_value()),
            verify_certs=config.verify_certs,
            ca_certs=config.ca_certs,
            use_ssl=config.use_ssl,
        )
        assert self._os.ping()

    def get_indices(self, index: Optional[str] = None, h=None) -> list:
        return self._os.cat.indices(format="json", index=index, h=h)

    def get_mapping(self, index_name: Optional[str] = None) -> dict:
        return self._os.indices.get_mapping(index=index_name)

    def get_index_settings(self, index_name: str) -> dict:
        return self._os.indices.get_settings(index=index_name)

    def get_data_streams(self, name: Optional[str] = None) -> dict:
        response = self._os.indices.get_data_stream(name=name)
        return response["data_streams"]

    def get_index_template(self, template_name: str) -> list[dict]:
        return self._os.indices.get_index_template(name=template_name).get(
            "index_templates"
        )
