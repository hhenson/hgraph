import asyncio

import tornado.web
from frozendict import frozendict

from hgraph.adaptors.tornado.http_server_adaptor import HttpGetRequest


class InspectorHttpHandler(tornado.web.RequestHandler):
    def initialize(self, queue):
        self.queue = queue

    async def get(self, *args):
        request = HttpGetRequest(
                url=self.request.uri,
                url_parsed_args=args,
                headers=self.request.headers,
                query=frozendict({k: ''.join(i.decode() for i in v) for k, v in self.request.query_arguments.items()}),
                cookies=frozendict(self.request.cookies))

        future = asyncio.Future()
        self.queue((future, request))

        response = await future

        self.set_status(response.status_code)

        if response.headers:
            for k, v in response.headers.items():
                self.set_header(k, v)

        await self.finish(response.body)
