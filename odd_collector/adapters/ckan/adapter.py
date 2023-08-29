from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import CKANGenerator

from odd_collector.domain.plugin import CKANPlugin

from .client import CKANRestClient
from .mappers.models import CKANResource
from .mappers.organization import map_organization
from .mappers.dataset import map_dataset
from .mappers.resource import map_resource


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
        resources_entities: list[DataEntity] = []

        try:
            for organization_name in organizations:
                datasets_entities_tmp: list[DataEntity] = []
                organization = await self.client.get_organization_details(
                    organization_name
                )
                datasets = await self.client.get_datasets(organization.id)
                for dataset in datasets:
                    self.oddrn_generator.set_oddrn_paths(
                        organizations=organization_name,
                        datasets=dataset.name,
                    )
                    resources = [
                        CKANResource(resource) for resource in dataset.resources
                    ]
                    resources_entities_tmp = [
                        map_resource(self.oddrn_generator, resource)
                        for resource in resources
                    ]

                    datasets_entities_tmp.append(
                        map_dataset(
                            self.oddrn_generator, dataset, resources_entities_tmp
                        )
                    )
                    resources_entities.extend(resources_entities_tmp)

                organization_entities.append(
                    map_organization(
                        self.oddrn_generator, organization, datasets_entities_tmp
                    )
                )
                datasets_entities.extend(datasets_entities_tmp)

        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*resources_entities, *datasets_entities, *organization_entities],
        )
