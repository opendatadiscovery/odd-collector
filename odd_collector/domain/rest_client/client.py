from asyncio import create_task, gather
from typing import Any, Callable, NamedTuple, Optional

from aiohttp import ClientSession


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[dict[Any, Any]] = None
    headers: Optional[dict[Any, Any]] = None
    payload: Optional[dict[Any, Any]] = None


class RestClient:
    @staticmethod
    async def fetch(
        session: ClientSession, request_args: RequestArgs
    ) -> dict[Any, Any]:
        async with session.request(
            request_args.method,
            url=request_args.url,
            params=request_args.params,
            headers=request_args.headers,
            json=request_args.payload,
        ) as response:
            return await response.json()

    async def fetch_all(self, requests: list[RequestArgs]) -> list[dict[Any, Any]]:
        async with ClientSession() as session:
            tasks = [create_task(self.fetch(session, req)) for req in requests]

            return await gather(*tasks)

    @staticmethod
    async def collect_nodes_with_pagination(
        default_page_size: int, cb: Callable, start_page_number: int = 0
    ):
        nodes_list = []
        current_page = start_page_number
        results_len = default_page_size

        while results_len == default_page_size:
            result = await cb(current_page)
            nodes_list += result
            results_len = len(result)
            current_page += 1

        return nodes_list
