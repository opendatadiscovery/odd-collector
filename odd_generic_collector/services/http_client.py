from aiohttp import ClientSession


class HttpClient:
    async def post(self, url: str, data: any, session: ClientSession, **kwargs):
        headers = {"content-type": "application/json"}
        resp = await session.post(url=url, data=data, headers=headers, **kwargs)
        return resp
