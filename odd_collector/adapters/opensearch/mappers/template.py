from funcy import get_in
from odd_models.models import DataEntity, DataEntityType, DataSet, DataTransformer
from oddrn_generator import ElasticSearchGenerator

from .fields import map_field
from .metadata import extract_template_metadata


class TemplateEntity(DataEntity):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data_transformer = DataTransformer(inputs=[], outputs=[])
        self.dataset = DataSet(field_list=[])

    def add_input(self, data_entity: DataEntity):
        if data_entity.oddrn not in self.data_transformer.inputs:
            self.data_transformer.inputs.append(data_entity.oddrn)

    def add_output(self, data_entity: DataEntity):
        if data_entity.oddrn not in self.data_transformer.outputs:
            self.data_transformer.outputs.append(data_entity.oddrn)


def map_template(template: dict, generator: ElasticSearchGenerator) -> TemplateEntity:
    generator.set_oddrn_paths(templates=template["name"])
    entity = TemplateEntity(
        oddrn=generator.get_oddrn_by_path("templates"),
        name=template["name"],
        owner=None,
        type=DataEntityType.FILE,
        metadata=[extract_template_metadata(template["index_template"])],
    )

    if properties := get_in(
        template, ["index_template", "template", "mappings", "properties"]
    ):
        fields = [
            map_field(name, value, generator, "templates_fields")
            for name, value in properties.items()
            if not name.startswith("@")
        ]
        if entity.dataset is None:
            entity.dataset = DataSet(field_list=fields)
        else:
            entity.dataset.field_list = fields

    return entity
