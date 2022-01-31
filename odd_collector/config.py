import pydantic


class Config(pydantic.BaseSettings):
    FLASK_ENVIRONMENT: str = "development"
    FLASK_DEBUG: bool = False


config = Config()
