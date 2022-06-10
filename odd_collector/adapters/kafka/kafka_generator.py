from typing import Optional

from oddrn_generator.generators import Generator
from oddrn_generator.path_models import BasePathsModel
from oddrn_generator.server_models import HostnameModel


class KafkaPathsModel(BasePathsModel):
    clusters: str
    topics: Optional[str]
    columns: Optional[str]

    class Config:
        dependencies_map = {
            'clusters': ('clusters',),
            'topics': ('clusters', 'topics'),
            'columns': ('clusters', 'topics', 'columns')
        }
        data_source_path = 'clusters'


class KafkaGenerator(Generator):
    source = "kafka"
    paths_model = KafkaPathsModel
    server_model = HostnameModel
