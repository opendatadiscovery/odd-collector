from asyncio import gather
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple

from aiohttp import ClientSession


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[Dict[Any, Any]] = None
    headers: Optional[Dict[Any, Any]] = None
    payload: Optional[Dict[Any, Any]] = None


class RestClient:
    @staticmethod
    async def fetch_async_response(
        session, request_args: RequestArgs
    ) -> Dict[Any, Any]:
        async with session.request(
            request_args.method,
            url=request_args.url,
            params=request_args.params,
            headers=request_args.headers,
            json=request_args.payload,
        ) as response:
            return await response.json()

    async def fetch_all_async_responses(
        self, request_args_list: List[RequestArgs]
    ) -> Tuple:
        async with ClientSession() as session:
            return await gather(
                *[
                    self.fetch_async_response(session, request_args=request_args)
                    for request_args in request_args_list
                ],
                return_exceptions=True,
            )

    @staticmethod
    async def collect_nodes_with_pagination(
        default_page_size: int, fetch_function: Callable
    ):
        nodes_list = []
        pg = 0
        results_len = default_page_size
        while results_len == default_page_size:
            result = await fetch_function(pg)
            nodes_list += result
            results_len = len(result)
            pg += 1
        return nodes_list
