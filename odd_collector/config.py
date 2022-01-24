import logging
import os
from typing import Any


class MissingEnvironmentVariable(Exception):
    pass


def get_env(env: str, default_value: Any = None) -> str:
    try:
        return os.environ[env]
    except KeyError:
        if default_value is not None:
            return default_value
        raise MissingEnvironmentVariable(f"{env} does not exist")


class BaseConfig:
    AWS_REGION: str = get_env('AWS_REGION')
    EXCLUDE_TABLES: list = get_env('EXCLUDE_TABLES', '').split(',')
    SCHEDULER_INTERVAL_MINUTES = get_env('SCHEDULER_INTERVAL_MINUTES', 60)


class DevelopmentConfig(BaseConfig):
    FLASK_DEBUG = True


class ProductionConfig(BaseConfig):
    FLASK_DEBUG = False


def log_env_vars(config: dict):
    logging.info('Environment variables:')
    logging.info(f'AWS_REGION={config["AWS_REGION"]}')
    logging.info(f'EXCLUDE_TABLES={config["EXCLUDE_TABLES"]}')
    logging.info(f'SCHEDULER_INTERVAL_MINUTES={config["SCHEDULER_INTERVAL_MINUTES"]}')
