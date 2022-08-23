from abc import ABC
from typing import List

from aiohttp import ClientSession
from odd_collector_sdk.errors import DataSourceAuthorizationError

from ...domain.plugin import CubeJSPlugin
from .domain.cube import Cube


class CubeJsRepositoryBase(ABC):
    def __init__(self, config: CubeJSPlugin):
        self.config = config
        self._host = config.host
        self._dev_mode = config.dev_mode
        self._token = config.token

    def get_cubes(self) -> List[Cube]:
        raise NotImplementedError


class CubeJsRepository(CubeJsRepositoryBase):
    API_PATH = "cubejs-api/v1"

    async def get_cubes(self) -> List[Cube]:
        headers = (
            None
            if self._dev_mode
            else {"Authorization": self._token.get_secret_value()}
        )

        async with ClientSession(headers=headers) as session:
            try:
                async with session.get(
                    f"{self._host}/{self.API_PATH}/meta?extended"
                ) as response:
                    result = await response.json()
                    return [Cube.from_response(cube) for cube in result.get("cubes")]
            except Exception as e:
                raise DataSourceAuthorizationError(
                    f"Couldn't connect to cube: {self._host}"
                ) from e
