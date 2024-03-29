from collections import defaultdict

from odd_collector.domain.plugin import CKANPlugin
from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_collector_sdk.errors import MappingDataError, DataSourceError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import CKANGenerator


from .client import CKANRestClient
from .mappers.group import map_group
from .mappers.organization import map_organization
from .mappers.dataset import map_dataset
from .mappers.resource import map_resource
from .utils import group_dataset_oddrns


class Adapter(AsyncAbstractAdapter):
    def __init__(self, config: CKANPlugin) -> None:
        self.oddrn_generator = CKANGenerator(host_settings=f"{config.host}")
        self.client = CKANRestClient(config)

    def get_data_source_oddrn(self) -> str:
        return self.oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        organizations = await self.client.get_organizations()
        groups = await self.client.get_groups()
        grouped_datasets_oddrns = defaultdict(list)
        organization_entities: list[DataEntity] = []
        datasets_entities: list[DataEntity] = []
        resources_entities: list[DataEntity] = []
        groups_entities: list[DataEntity] = []

        try:
            for organization in organizations:
                datasets_entities_tmp: list[DataEntity] = []
                datasets = await self.client.get_datasets(organization.id)
                for dataset in datasets:

                    resources_entities_tmp = []
                    self.oddrn_generator.set_oddrn_paths(
                        organizations=organization.name,
                        datasets=dataset.name,
                    )
                    group_dataset_oddrns(
                        self.oddrn_generator,
                        dataset.name,
                        dataset.groups,
                        grouped_datasets_oddrns,
                    )
                    for resource in dataset.resources:
                        fields = await self.client.get_resource_fields(resource.id)
                        resources_entities_tmp.append(
                            map_resource(self.oddrn_generator, resource, fields)
                        )

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

            for group_name in groups:
                group = await self.client.get_group_details(group_name)
                groups_entities.append(
                    map_group(
                        self.oddrn_generator, group, grouped_datasets_oddrns[group_name]
                    )
                )

        except DataSourceError:
            raise

        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[
                *resources_entities,
                *datasets_entities,
                *organization_entities,
                *groups_entities,
            ],
        )
