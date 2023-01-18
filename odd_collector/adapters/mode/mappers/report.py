from odd_models.models import DataEntity

from ..domain.report import Report
from ..generator import ModeGenerator


# TODO: complete me
def map_report(
        oddrn_generator: ModeGenerator, report: Report
) -> DataEntity:
    return Report
