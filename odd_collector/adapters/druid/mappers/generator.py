from oddrn_generator import Generator
from oddrn_generator.server_models import HostnameModel

from odd_collector.adapters.druid.mappers.path_model import DruidPathsModel


class DruidGenerator(Generator):
    source = "druid"
    paths_model = DruidPathsModel
    server_model = HostnameModel
