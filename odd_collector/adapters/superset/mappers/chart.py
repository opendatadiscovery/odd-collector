from odd_models.models import DataConsumer, DataEntity, DataEntityType
from oddrn_generator.generators import SupersetGenerator

from ..domain.chart import Chart


def map_chart(
    generator: SupersetGenerator,
    chart: Chart,
) -> DataEntity:
    name = chart.slice_name
    generator.set_oddrn_paths(charts=chart.id)
    oddrn = generator.get_oddrn_by_path("charts")

    return DataEntity(
        oddrn=oddrn,
        name=name,
        owner=None,
        description=None,
        metadata=[],
        type=DataEntityType.DASHBOARD,
        data_consumer=DataConsumer(inputs=[]),
    )
