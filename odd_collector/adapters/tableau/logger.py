from funcy.flow import decorator
from odd_collector_sdk.logger import logger
from odd_models.models import DataEntity

logger = logger


@decorator
def log_entity(call):
    entity: DataEntity = call()
    logger.debug(entity.json(exclude_none=True, exclude_unset=True))
    return entity
