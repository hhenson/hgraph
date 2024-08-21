import urllib
from typing import Callable
from urllib.parse import urlencode

from tornado.httpclient import AsyncHTTPClient

from hgraph import service_adaptor, TS, service_adaptor_impl, TSD, push_queue, GlobalState, sink_node
from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.http_server_adaptor import HttpRequest, HttpResponse, HttpPostRequest


@service_adaptor
def http_client_adaptor(request: TS[HttpRequest], path: str = "http_client") -> TS[HttpResponse]: ...


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]], path: str = "http_client"
) -> TSD[int, TS[HttpResponse]]:

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

        if isinstance(request, HttpPostRequest):
            response = await client.fetch(
                url, method="POST", headers=request.headers, body=request.body, raise_error=False
            )
        else:
            response = await client.fetch(url, method="GET", headers=request.headers, raise_error=False)

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
