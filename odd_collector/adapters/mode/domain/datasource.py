from dataclasses import dataclass
from typing import Optional, Any, Dict
from ..generator import Generator


@dataclass
class DataSource:

    id: int
    name: str
    description: str
    token: str
    adapter: str
    created_at: str
    updated_at: str
    has_expensive_schema_updates: bool
    public: bool
    asleep: bool
    queryable: bool
    display_name: str
    database: str
    host: str
    port: str
    ssl: bool
    username: str
    provider: str
    vendor: str
    ldap: bool
    warehouse: str
    bridged: bool
    adapter_version: str
    custom_attributes: dict
    _links: dict

    account_id: Optional[int]
    account_username: Optional[str]
    organization_token: Optional[str]
    default: Optional[str]
    default_for_organization_id: Optional[str]
    ssl_trusted_cert: Optional[str]
    default_access_level: Optional[str]

    @staticmethod
    def from_response(response: Dict[str, Any]):
        datasource = DataSource(
            id=response.get("id"),
            name=response.get("name"),
            description=response.get("description"),
            token=response.get("token"),
            adapter=response.get("adapter"),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            has_expensive_schema_updates=response.get("has_expensive_schema_updates"),
            public=response.get("public"),
            asleep=response.get("asleep"),
            queryable=response.get("queryable"),
            display_name=response.get("display_name"),
            database=response.get("database"),
            host=response.get("host"),
            port=response.get("port"),
            ssl=response.get("ssl"),
            username=response.get("username"),
            provider=response.get("provider"),
            vendor=response.get("vendor"),
            ldap=response.get("ldap"),
            warehouse=response.get("warehouse"),
            bridged=response.get("bridged"),
            adapter_version=response.get("adapter_version"),
            custom_attributes=response.get("custom_attributes"),
            _links=response.get("_links"),

            account_id=response.get("account_id"),
            account_username=response.get("account_username"),
            organization_token=response.get("organization_token"),
            default=response.get("default"),
            default_for_organization_id=response.get("default_for_organization_id"),
            ssl_trusted_cert=response.get("ssl_trusted_cert"),
            default_access_level=response.get("default_access_level"),

        )
        return datasource

    def get_oddrn(self, oddrn_generator: Generator):
        oddrn_generator.get_oddrn_by_path("datasource", self.name)
        return oddrn_generator.get_oddrn_by_path("datasource")
