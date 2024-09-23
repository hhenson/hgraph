from collections import namedtuple
from logging import getLogger
from typing import Callable
from urllib.parse import urlencode
from frozendict import frozendict as fd

from tornado.httpclient import AsyncHTTPClient

from hgraph import service_adaptor, TS, service_adaptor_impl, TSD, push_queue, GlobalState, sink_node
from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.http_server_adaptor import (
    HttpRequest,
    HttpResponse,
    HttpPostRequest,
    HttpGetRequest,
    HttpPutRequest,
    HttpDeleteRequest,
)


@service_adaptor
def http_client_adaptor(request: TS[HttpRequest], path: str = "http_client") -> TS[HttpResponse]: ...


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]], path: str = "http_client"
) -> TSD[int, TS[HttpResponse]]:
    logger = getLogger("hgraph")
    logger.info("Starting client adaptor on path: '%s'", path)

    @push_queue(TSD[int, TS[HttpResponse]])
    def from_web(sender, path: str = "http_client") -> TSD[int, TS[HttpResponse]]:
        GlobalState.instance()[f"http_client_adaptor://{path}/queue"] = sender
        return None

    async def make_http_request(id: int, request: HttpRequest, sender: Callable):
        client = AsyncHTTPClient()
        if request.query:
            url = f"{request.url}?{urlencode(request.query)}"
        else:
            url = request.url

        if isinstance(request, HttpGetRequest):
            logger.debug("[GET][%s]", url)
            response = await client.fetch(url, method="GET", headers=request.headers, raise_error=False)
        elif isinstance(request, HttpPostRequest):
            logger.debug("[POST][%s] body: %s", url, request.body)
            response = await client.fetch(
                url, method="POST", headers=request.headers, body=request.body, raise_error=False
            )
        elif isinstance(request, HttpPutRequest):
            logger.debug("[PUT][%s] body: %s", url, request.body)
            response = await client.fetch(
                url, method="PUT", headers=request.headers, body=request.body, raise_error=False
            )
        elif isinstance(request, HttpDeleteRequest):
            logger.debug("[DELETE][%s]", url)
            response = await client.fetch(url, method="DELETE", headers=request.headers, raise_error=False)
        else:
            logger.error("Bad request received: %s", request)
            response = namedtuple("HttpResponse_", ["code", "headers", "body"])(
                400, fd(), b"Incorrect request type provided"
            )

        sender({id: HttpResponse(status_code=response.code, headers=response.headers, body=response.body.decode())})

    @sink_node
    def to_web(request: TSD[int, TS[HttpRequest]]):
        sender = GlobalState.instance()[f"http_client_adaptor://{path}/queue"]

        for i, r in request.modified_items():
            TornadoWeb.get_loop().add_callback(make_http_request, i, r.value, sender)

    @to_web.start
    def to_web_start():
        TornadoWeb.start_loop()

    @to_web.stop
    def to_web_stop():
        TornadoWeb.stop_loop()

    to_web(request)
    return from_web()
