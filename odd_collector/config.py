import pydantic


class Config(pydantic.BaseSettings):
    FLASK_ENVIRONMENT: str = "development"
    FLASK_DEBUG: bool = False
    platform_host_url: str = "http://odd-platform:8080"


config = Config()
