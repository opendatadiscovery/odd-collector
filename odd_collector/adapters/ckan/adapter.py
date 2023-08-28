import asyncio
from collections import defaultdict

from funcy import group_by
from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import CKANGenerator

from odd_collector.domain.plugin import CKANPlugin

from .client import CKANRestClient
from .mappers.organization import map_organization
from .mappers.dataset import map_dataset
from .logger import logger


class Adapter(AsyncAbstractAdapter):
    def __init__(self, config: CKANPlugin) -> None:
        self.oddrn_generator = CKANGenerator(host_settings=f"{config.host}")
        self.client = CKANRestClient(config)

    def get_data_source_oddrn(self) -> str:
        return self.oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        organizations = await self.client.get_organizations()
        organization_entities: list[DataEntity] = []
        datasets_entities: list[DataEntity] = []

        try:
            for organization_name in organizations:
                self.oddrn_generator.set_oddrn_paths(
                    organizations=organization_name,
                )
                organization_raw = await self.client.get_organization_details(
                    organization_name
                )
                organization_id = organization_raw["id"]
                datasets_raw = await self.client.get_datasets(organization_id)
                datasets_entities_tmp = [
                    map_dataset(self.oddrn_generator, dataset)
                    for dataset in datasets_raw
                ]
                organization_entities.append(
                    map_organization(
                        self.oddrn_generator, organization_name, datasets_entities_tmp
                    )
                )
                datasets_entities.extend(datasets_entities_tmp)

        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*datasets_entities, *organization_entities],
        )
