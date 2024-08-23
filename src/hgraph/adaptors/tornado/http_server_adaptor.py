import asyncio
import concurrent.futures
from dataclasses import dataclass, field
from typing import Callable

from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    graph,
    push_queue,
    TSD,
    TS,
    GlobalState,
    sink_node,
    STATE,
    partition,
    map_,
    unpartition,
    adaptor,
    combine,
    adaptor_impl,
    merge,
    register_adaptor,
    register_service,
    TSB,
    TS_SCHEMA,
    TIME_SERIES_TYPE,
    HgTSBTypeMetaData,
    nothing,
)
import tornado
from hgraph.adaptors.tornado._tornado_web import TornadoWeb


@dataclass(frozen=True)
class HttpRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str] = ()
    query: dict[str, str] = frozendict()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, str] = frozendict()


@dataclass(frozen=True)
class HttpResponse(CompoundScalar):
    status_code: int
    headers: frozendict[str, str] = frozendict()
    cookies: frozendict[str, str] = frozendict()
    body: str = ""


@dataclass(frozen=True)
class HttpGetRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpPostRequest(HttpRequest):
    body: str = ""


class HttpAdaptorManager:
    handlers: dict[str, Callable | str]

    def __init__(self):
        self.handlers = {}
        self.requests = {}

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def set_queue(self, queue):
        self.queue = queue

    def start(self, port):
        self.tornado_web = TornadoWeb.instance(port)

        for path in self.handlers.keys():
            self.tornado_web.add_handler(path, HttpHandler, {"path": path, "mgr": self})

        self.tornado_web.start()

    def stop(self):
        self.tornado_web.stop()

    def add_handler(self, path, handler):
        self.handlers[path] = handler

    def add_request(self, request_id, request):
        try:
            future = asyncio.Future()
        except Exception as e:
            print(f"Error creating future: {e}")
            raise e
        self.requests[request_id] = future
        self.queue({request_id: request})
        return future

    def complete_request(self, request_id, response):
        self.requests[request_id].set_result(response)
        print(f"Completed request {request_id} with response {response}")


class HttpHandler(tornado.web.RequestHandler):
    def initialize(self, path, mgr):
        self.path = path
        self.mgr = mgr

    async def get(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response = await self.mgr.add_request(
            request_id, HttpGetRequest(
                url=self.path,
                url_parsed_args=args,
                headers=self.request.headers,
                query=frozendict({k: ''.join(i.decode() for i in v) for k, v in self.request.query_arguments.items()}),
                cookies=frozendict(self.request.cookies))
        )

        self.set_status(response.status_code)

        if response.headers:
            for k, v in response.headers.items():
                self.set_header(k, v)

        await self.finish(response.body)

    async def post(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response = await self.mgr.add_request(
            request_id,
            HttpPostRequest(url=self.path, url_parsed_args=args, headers=self.request.headers, body=self.request.body),
        )

        self.set_status(response.status_code)
        for k, v in response.headers.items():
            self.set_header(k, v)
        await self.finish(response.body)


def http_server_handler(fn: Callable = None, *, url: str):
    if fn is None:
        return lambda fn: http_server_handler(fn, url=url)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "request" in fn.signature.time_series_inputs.keys(), "Http graph must have an input named 'request'"
    assert fn.signature.time_series_inputs["request"].matches_type(TS[HttpRequest]) or fn.signature.time_series_inputs[
        "request"
    ].matches_type(
        TSD[int, TS[HttpRequest]]
    ), "Http graph must have a single input named 'request' of type TS[HttpRequest] or TSD[int, TS[HttpRequest]]"

    output_type = fn.signature.output_type
    if isinstance(output_type, HgTSBTypeMetaData):
        output_type = output_type["response"]

    assert output_type.matches_type(TS[HttpResponse]) or output_type.matches_type(
        TSD[int, TS[HttpResponse]]
    ), "Http graph must have a single output of type TS[HttpResponse] or TSD[int, TS[HttpResponse]]"

    mgr = HttpAdaptorManager.instance()
    # this makes the handler to be auto-wired in the http_server_adaptor
    mgr.add_handler(url, fn)

    @graph
    def http_server_handler_graph(**inputs: TSB[TS_SCHEMA]) -> TIME_SERIES_TYPE:
        # if however this is wired into the graph explicitly, it will be used instead of the auto-wiring the handler
        mgr.add_handler(url, None)  # prevent auto-wiring

        requests = http_server_adaptor.to_graph(path=url, __no_ts_inputs__=True)
        if fn.signature.time_series_inputs["request"].matches_type(TS[HttpRequest]):
            if inputs.as_dict():
                responses = map_(lambda r, i: fn(request=r, **i.as_dict()), requests, inputs)
            else:
                responses = map_(lambda r: fn(request=r), requests)
        else:
            responses = fn(request=requests, **inputs)

        if isinstance(responses.output_type, HgTSBTypeMetaData):
            http_server_adaptor.from_graph(responses.response, path=url)
            return responses
        else:
            http_server_adaptor.from_graph(responses, path=url)
            return combine()

    return http_server_handler_graph


@adaptor
def http_server_adaptor(response: TSD[int, TS[HttpResponse]], path: str) -> TSD[int, TS[HttpRequest]]: ...


@adaptor_impl(interfaces=(http_server_adaptor, http_server_adaptor))
def http_server_adaptor_helper(path: str, port: int):
    register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)


@adaptor_impl(interfaces=())
def http_server_adaptor_impl(path: str, port: int):
    from hgraph import WiringNodeClass
    from hgraph import WiringGraphContext

    @push_queue(TSD[int, TS[HttpGetRequest]])
    def from_web(sender, path: str = "tornado_http_server_adaptor") -> TSD[int, TS[HttpGetRequest]]:
        GlobalState.instance()[f"http_server_adaptor://{path}/queue"] = sender
        return None

    @sink_node
    def to_web(
        responses: TSD[int, TS[HttpResponse]],
        port: int,
        path: str = "tornado_http_server_adaptor",
        _state: STATE = None,
    ):
        for response_id, response in responses.modified_items():
            TornadoWeb.get_loop().add_callback(_state.mgr.complete_request, response_id, response.value)

    @to_web.start
    def to_web_start(port: int, path: str, _state: STATE):
        _state.mgr = HttpAdaptorManager.instance()
        _state.mgr.set_queue(queue=GlobalState.instance()[f"http_server_adaptor://{path}/queue"])
        _state.mgr.start(port)

    @to_web.stop
    def to_web_stop(_state: STATE):
        _state.mgr.tornado_web.stop()

    requests = from_web()
    requests_by_url = partition(requests, requests.url)

    responses = {}
    for url, handler in HttpAdaptorManager.instance().handlers.items():
        if isinstance(handler, WiringNodeClass):
            if handler.signature.time_series_inputs["request"].matches_type(TS[HttpGetRequest]):
                responses[url] = map_(handler, request=requests_by_url[url])
            elif handler.signature.time_series_inputs["request"].matches_type(TSD[int, TS[HttpGetRequest]]):
                responses[url] = handler(request=requests_by_url[url])
        elif handler is None:
            pass
        else:
            raise ValueError(f"Invalid REST handler type for the http_ adaptor: {handler}")

    adaptors_dedup = set()
    adaptors = set()
    for path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(
        http_server_adaptor
    ):
        assert type_map == {}, "Http adaptor does not support type generics"
        if (path, receive) in adaptors_dedup:
            raise ValueError(f"Duplicate http_ adaptor client for path {path}: only one client is allowed")
        adaptors_dedup.add((path, receive))
        adaptors.add(path.replace("/from_graph", "").replace("/to_graph", ""))

    for path in adaptors:
        url = http_server_adaptor.path_from_full_path(path)

        mgr = HttpAdaptorManager.instance()
        mgr.add_handler(url, None)

        responses[url] = http_server_adaptor.wire_impl_inputs_stub(path).response
        http_server_adaptor.wire_impl_out_stub(path, requests_by_url[url])

    to_web(merge(*responses.values()), port)
