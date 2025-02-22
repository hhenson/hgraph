import asyncio
from abc import ABC
from dataclasses import dataclass
from logging import info, getLogger
from typing import Callable

import tornado
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
    adaptor,
    combine,
    adaptor_impl,
    merge,
    register_adaptor,
    TSB,
    TS_SCHEMA,
    TIME_SERIES_TYPE,
    HgTSBTypeMetaData,
    HgTSDTypeMetaData,
    REMOVE_IF_EXISTS,
    LOGGER,
    default_path,
)
from hgraph.adaptors.tornado._tornado_web import TornadoWeb

__all__ = (
    "HttpRequest",
    "HttpResponse",
    "HttpGetRequest",
    "HttpDeleteRequest",
    "HttpPostRequest",
    "HttpPutRequest",
    "HttpAdaptorManager",
    "HttpHandler",
    "http_server_handler",
    "http_server_adaptor",
    "register_http_server_adaptor",
)


@dataclass(frozen=True)
class HttpRequest(CompoundScalar):
    """
    NOTE: Do not use this class directly.
    Use one of the specific types i.e.:
    * HttpGetRequest
    * HttpPutRequest
    * HttpDeleteRequest
    * HttpPostRequest

    Using this will result in a request failure with code 400.
    """

    url: str
    url_parsed_args: tuple[str, ...] = ()
    query: dict[str, str] = frozendict()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None


@dataclass(frozen=True)
class HttpResponse(CompoundScalar):
    status_code: int
    headers: frozendict[str, str] = frozendict()
    cookies: frozendict[str, dict[str, object]] = frozendict()
    body: str = ""


@dataclass(frozen=True)
class HttpGetRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpDeleteRequest(HttpRequest):
    pass


@dataclass(frozen=True)
class HttpPutRequest(HttpRequest):
    body: str = ""


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

    def remove_request(self, request_id):
        self.queue({request_id: REMOVE_IF_EXISTS})
        del self.requests[request_id]


class HttpHandler(tornado.web.RequestHandler):
    def initialize(self, path, mgr):
        self.path = path
        self.mgr: HttpAdaptorManager = mgr

    async def get(self, *args):
        request_obj = HttpGetRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            query=frozendict({k: "".join(i.decode() for i in v) for k, v in self.request.query_arguments.items()}),
            cookies=frozendict({k: frozendict({'value': v.value, **{p: w for p, w in v.items()}}) for k, v in self.request.cookies.items()}),
        )
        await self._handle_request(request_obj)

    async def delete(self, *args):
        request_obj = HttpDeleteRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            cookies=frozendict({k: frozendict({'value': v.value, **{p: w for p, w in v.items()}}) for k, v in self.request.cookies.items()}),
        )
        await self._handle_request(request_obj)

    async def post(self, *args):
        request_obj = HttpPostRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            body=self.request.body.decode("utf-8"),
            cookies=frozendict({k: frozendict({'value': v.value, **{p: w for p, w in v.items()}}) for k, v in self.request.cookies.items()}),
        )
        await self._handle_request(request_obj)

    async def put(self, *args):
        request_obj = HttpPutRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            body=self.request.body.decode("utf-8"),
            cookies=frozendict({k: frozendict({'value': v.value, **{p: w for p, w in v.items()}}) for k, v in self.request.cookies.items()}),
        )
        await self._handle_request(request_obj)

    async def _handle_request(self, request_obj):
        response = await self.mgr.add_request(
            id(request_obj),
            request_obj,
        )
        self.set_status(response.status_code)
        for k, v in response.headers.items():
            self.set_header(k, v)
        for k, v in response.cookies.items():
            if isinstance(v, str):
                self.set_cookie(k, v)
            elif isinstance(v, dict):
                self.set_cookie(k, **v)
        await self.finish(response.body)
        self.mgr.remove_request(id(request_obj))


def http_server_handler(fn: Callable = None, *, url: str):
    """
    Wrap an endpoint or route in the adaptor handler.
    If the handler is simple (i.e. it is self-contained) then the function can have a simple signature of:

    ::

        @http_server_handler(url='/mypath')
        def simple_handler(request: TS[HttpRequest]) -> TS[HttpResponse]:
            return combine[TS[HttpResponse]](status_code=200, body="Simple Response")

    In this case, so long as this is imported, it will be wired in when the server is registered.

    The http server is registered as below:

    ::

        register_adaptor(default_path, http_server_adaptor_helper, port=8081)

    The handler can take single ``TS`` inputs as above, or it can process all requests simultaneously. In this
    mode, the signature takes the form below:

    ::

        @http_server_handler(url='/mypath')
        def batch_handler(request: TSD[int, TS[HttpRequest]]) -> TSD[int, TS[HttpResponse]]:
            ...

    In this more each request is collected into a ``TSD``. As a reminder, it is the implementers' responsibility
    to process the removal requests (forwarding them to the response output.

    It is possible to support additional inputs and outputs. For inputs, add them to the input signature. Once
    the handler has any additional inputs (other than ``request``) the handler must be instantiated manually.
    To return more than one response, use a TSB to encapsulate the responses. Note that there must be at least
    one response that is called response and has a type of ``TS[HttpResponse]`` or ``TSD[int, TS[HttpResponse]]``.
    For example:

    ::

        class MyHandlerResponse(TimeSeriesSchema):
            response: TS[HttpResponse]
            p1: ...
            p2: ...

        @http_server_handler(url='/mypath')
        def complex_handler(request: TS[HttpRequest], ts_1: ...) -> TSB[MyHandlerResponse]:
            ...

    To instantiate the handler, the function is called in the graph with the inputs being the list
    of parameters in the input other than the ``request`` parameter. For example:

    ::
        register_adaptor(default_path, http_server_adaptor_helper, port=8081)
        ...
        out = complex_handlder(ts_1=..., ...)

    The ``request`` argument is automatically wired into the adaptor. The response is returned in full, but the
    ``response`` output will also be wired into the adaptor.
    """
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
    is_tsb = False
    if isinstance(output_type, HgTSBTypeMetaData):
        is_tsb = True
        output_type = output_type["response"]

    assert output_type.matches_type(TS[HttpResponse]) or output_type.matches_type(
        TSD[int, TS[HttpResponse]]
    ), "Http graph must have a single output of type TS[HttpResponse] or TSD[int, TS[HttpResponse]]"

    if not is_tsb and len(fn.signature.non_defaulted_arguments) == 1:
        # this makes the handler to be auto-wired in the http_server_adaptor
        HttpAdaptorManager.instance().add_handler(url, fn)

    @graph
    def http_server_handler_graph(**inputs: TSB[TS_SCHEMA]) -> TIME_SERIES_TYPE:
        # if however this is wired into the graph explicitly, it will be used instead of the auto-wiring the handler
        HttpAdaptorManager.instance().add_handler(url, None)  # prevent auto-wiring

        requests = http_server_adaptor.to_graph(path=url, __no_ts_inputs__=True)
        if fn.signature.time_series_inputs["request"].matches_type(TS[HttpRequest]):
            if inputs.as_dict():
                responses = map_(lambda r, i: fn(request=r, **i.as_dict()), requests, inputs)
            else:
                responses = map_(lambda r: fn(request=r), requests)
        else:
            responses = fn(request=requests, **inputs.as_dict())

        if isinstance(responses.output_type, HgTSBTypeMetaData) or (
            isinstance(responses.output_type, HgTSDTypeMetaData)
            and isinstance(responses.output_type.value_tp.dereference(), HgTSBTypeMetaData)
        ):
            http_server_adaptor.from_graph(responses.response, path=url)
            return responses
        else:
            http_server_adaptor.from_graph(responses, path=url)
            return combine()

    return http_server_handler_graph


@adaptor
def http_server_adaptor(response: TSD[int, TS[HttpResponse]], path: str) -> TSD[int, TS[HttpRequest]]: ...


def register_http_server_adaptor(port: int):
    """Correctly registers the http server adaptor and associated machinery."""
    register_adaptor(default_path, http_server_adaptor_helper, port=port)
    register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)


# NOTE: we define the interface 'twice' to trick the adaptor impl to assume this is a mult-service which allows
# us to "manually wire" the service. Bypassing the need to accept or be provided with inputs.
@adaptor_impl(interfaces=(http_server_adaptor, http_server_adaptor))
def http_server_adaptor_helper(path: str, port: int):
    """
    Use this with the ``default_path`` to support ``http_server_handler`` instances that make use of
    additional inputs other than request. This ensures the wiring service can correct resolve and wire the
    handlers into the server adapter.
    """


@adaptor_impl(interfaces=())
def http_server_adaptor_impl(path: str, port: int):
    """Don't use this directly, wire in using the http_server_adaptor_helper."""
    from hgraph import WiringNodeClass
    from hgraph import WiringGraphContext

    logger = getLogger("hgraph")
    logger.info("Wiring HTTP Server Adaptor on port %d", port)

    @push_queue(TSD[int, TS[HttpRequest]])
    def from_web(sender, path: str = "tornado_http_server_adaptor") -> TSD[int, TS[HttpRequest]]:
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
            logger.info("Adding handler: [%s] %s", url, handler.signature.signature)
            if handler.signature.time_series_inputs["request"].matches_type(TS[HttpRequest]):
                responses[url] = map_(handler, request=requests_by_url[url])
            elif handler.signature.time_series_inputs["request"].matches_type(TSD[int, TS[HttpRequest]]):
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

    to_web(merge(*responses.values(), disjoint=True), port)
