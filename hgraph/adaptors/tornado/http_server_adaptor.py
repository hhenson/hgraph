import asyncio
import logging
from dataclasses import dataclass
from typing import Callable

import tornado
import tornado.iostream
from frozendict import frozendict

from hgraph import (
    REMOVE_IF_EXISTS,
    STATE,
    TIME_SERIES_TYPE,
    TS,
    TS_SCHEMA,
    TSB,
    TSD,
    CompoundScalar,
    GlobalState,
    HgTSBTypeMetaData,
    HgTSDTypeMetaData,
    HgTypeMetaData,
    adaptor,
    adaptor_impl,
    combine,
    default_path,
    graph,
    map_,
    merge,
    partition,
    push_queue,
    register_adaptor,
    sink_node,
)
from hgraph.adaptors.tornado._tornado_web import BaseHandler, TornadoWeb

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


logger = logging.getLogger("http_server_adaptor")


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
    connect_timeout: float = 20.0  # in seconds
    request_timeout: float = 20.0  # in seconds


@dataclass(frozen=True)
class HttpResponse(CompoundScalar):
    status_code: int
    headers: frozendict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    body: bytes = b""

    async def write(self, stream):
        stream.write(self.body)

    def __repr__(self):
        return (
            f"HttpResponse(status_code={self.status_code}, headers={self.headers}, cookies={self.cookies},"
            f" body_length={len(self.body)})"
        )


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
        self._next_request_id = 1
        self._pyid_to_id = {}
        self._pending: list[tuple[int, HttpRequest]] = []
        self._registered_paths_by_port: dict[int, set[str]] = {}
        self.queue = None
        self._coalesce: dict[int, HttpRequest] = {}
        self._flush_scheduled: bool = False

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def set_queue(self, queue):
        self.queue = queue
        # Flush any buffered requests that arrived before the queue was available
        if self.queue is not None and getattr(self, "_pending", None):
            for rid, req in self._pending:
                logger.debug(f"Flushing buffered request rid={rid} path={getattr(req, 'url', '?')}")
                self.queue({rid: req})
            self._pending.clear()

    def start(self, port):
        self.tornado_web = TornadoWeb.instance(port)

        logger.info("HttpAdaptorManager.start: registering %d handler(s) on port %s", len(self.handlers), port)
        reg = self._registered_paths_by_port.setdefault(port, set())
        for path in self.handlers.keys():
            if path not in reg:
                logger.info("HttpAdaptorManager.start: add_handler path=%s", path)
                self.tornado_web.add_handler(path, HttpHandler, {"path": path, "mgr": self})
                reg.add(path)

        self.tornado_web.start()

    def _schedule_flush(self):
        # Schedule a single flush of coalesced requests onto the engine queue via Tornado IOLoop
        if self._flush_scheduled or self.queue is None or not self._coalesce:
            return
        self._flush_scheduled = True
        try:
            TornadoWeb.get_loop().add_callback(self._flush)
        except Exception:
            # If no loop yet, fallback to immediate flush
            self._flush()

    def _flush(self):
        try:
            if self.queue is None:
                return
            batch = dict(self._coalesce)
            if batch:
                self.queue(batch)
        finally:
            self._coalesce.clear()
            self._flush_scheduled = False

    def stop(self):
        self.tornado_web.stop()

    def add_handler(self, path, handler):
        self.handlers[path] = handler

    def add_request(self, request_id, request):
        # Map transient Python object id to a stable, unique request id to avoid id() reuse collisions
        pyid = request_id
        rid = self._pyid_to_id.get(pyid)
        if rid is None:
            rid = self._next_request_id
            self._next_request_id += 1
            self._pyid_to_id[pyid] = rid
        try:
            # Create a Future bound to the current running asyncio loop (Tornado 6 uses asyncio)
            future = asyncio.get_running_loop().create_future()
        except Exception:
            logger.debug("Falling back to asyncio.Future(); no running loop found", exc_info=True)
            future = asyncio.Future()
        self.requests[rid] = future
        # If the queue isn't ready yet, buffer the request and flush later in set_queue()
        if self.queue is None:
            logger.debug(f"Buffering request rid={rid} for path={getattr(request, 'url', '?')} (queue not ready)")
            self._pending.append((rid, request))
        else:
            logger.debug(f"Enqueue request rid={rid} for path={getattr(request, 'url', '?')}")
            self.queue({rid: request})
        return future

    def complete_request(self, request_id, response):
        if request_id in self.requests:
            request = self.requests[request_id]
            if not request.done():
                request.set_result(response)
                logger.info(
                    f"Completed request {request_id} with response"
                    f" {response if len(response.body) < 1000 else str(len(response.body)) + ' bytes response'}"
                )
            else:
                logger.warning(f"Request {request_id} already completed or cancelled.")

    def remove_request(self, request_pyid):
        # Translate transient Python object id back to the stable request id and clean up
        rid = self._pyid_to_id.pop(request_pyid, None)
        if rid is None:
            logger.debug(f"remove_request called with unknown pyid={request_pyid}")
            return
        # Do NOT push REMOVE into the request TSD; it can cause Sentinels to appear mid-cycle
        # and break partitioning/field access. Just clean local tracking.
        future = self.requests.pop(rid, None)
        if future is not None and not future.done():
            # Ensure we don't leak a pending future if the client disconnected
            future.cancel()

    def shutdown(self, path: str | None = None):
        """Cancel outstanding futures, clear internal state, and drop queue/global references."""
        # Cancel any outstanding futures
        for fut in list(self.requests.values()):
            try:
                if fut is not None and not fut.done():
                    fut.cancel()
            except Exception:
                logger.debug("Ignoring exception while cancelling pending future", exc_info=True)
        self.requests.clear()
        self._pyid_to_id.clear()
        # Clear any buffered requests
        try:
            self._pending.clear()
        except Exception:
            pass
        # Drop queue reference and GlobalState key if provided
        if path is not None:
            try:
                del GlobalState.instance()[f"http_server_adaptor://{path}/queue"]
            except Exception:
                pass
        self.queue = None


class HttpHandler(BaseHandler):
    def initialize(self, path, mgr):
        self.path = path
        self.mgr: HttpAdaptorManager = mgr

    async def get(self, *args):
        request_obj = HttpGetRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            query=frozendict({k: "".join(i.decode() for i in v) for k, v in self.request.query_arguments.items()}),
            cookies=frozendict({
                k: frozendict({"value": v.value, **{p: w for p, w in v.items()}})
                for k, v in self.request.cookies.items()
            }),
            auth=getattr(self, "current_user", None),
        )
        await self._handle_request(request_obj)

    async def delete(self, *args):
        request_obj = HttpDeleteRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            cookies=frozendict({
                k: frozendict({"value": v.value, **{p: w for p, w in v.items()}})
                for k, v in self.request.cookies.items()
            }),
            auth=getattr(self, "current_user", None),
        )
        await self._handle_request(request_obj)

    async def post(self, *args):
        request_obj = HttpPostRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            body=self.request.body.decode("utf-8"),
            cookies=frozendict({
                k: frozendict({"value": v.value, **{p: w for p, w in v.items()}})
                for k, v in self.request.cookies.items()
            }),
            auth=getattr(self, "current_user", None),
        )
        await self._handle_request(request_obj)

    async def put(self, *args):
        request_obj = HttpPutRequest(
            url=self.path,
            url_parsed_args=args,
            headers=self.request.headers,
            body=self.request.body.decode("utf-8"),
            cookies=frozendict({
                k: frozendict({"value": v.value, **{p: w for p, w in v.items()}})
                for k, v in self.request.cookies.items()
            }),
            auth=getattr(self, "current_user", None),
        )
        await self._handle_request(request_obj)

    async def _handle_request(self, request_obj):
        response = await self.mgr.add_request(
            id(request_obj),
            request_obj,
        )

        try:
            self.set_status(response.status_code)
            for k, v in response.headers.items():
                self.set_header(k, v)
            for k, v in response.cookies.items():
                if isinstance(v, str):
                    self.set_cookie(k, v)
                elif isinstance(v, dict):
                    self.set_cookie(k, **v)
            await response.write(self)
            await self.finish()
        except tornado.iostream.StreamClosedError:
            pass  # the client closed the connection before we could send the response

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

    if isinstance(output_type, HgTSDTypeMetaData) and isinstance(output_type.value_tp, HgTSBTypeMetaData):
        is_tsb = True
        output_type = HgTypeMetaData.parse_type(TSD[int, output_type.value_tp["response"]])

    assert output_type.matches_type(TS[HttpResponse]) or output_type.matches_type(
        TSD[int, TS[HttpResponse]]
    ), f"Http graph must have a single output of type TS[HttpResponse] or TSD[int, TS[HttpResponse]]: {output_type}"

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
    from hgraph import WiringGraphContext, WiringNodeClass

    logger.info("Wiring HTTP Server Adaptor on port %d", port)

    @push_queue(TSD[int, TS[HttpRequest]])
    def from_web(sender, path: str = "tornado_http_server_adaptor") -> TSD[int, TS[HttpRequest]]:
        # Store the queue sender in GlobalState for discovery and also set it directly on the manager
        GlobalState.instance()[f"http_server_adaptor://{path}/queue"] = sender
        try:
            HttpAdaptorManager.instance().set_queue(queue=sender)
        except Exception:
            # If the manager isn't ready yet, it will set the queue in to_web_start
            pass
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
    def to_web_stop(path: str, _state: STATE):
        try:
            _state.mgr.shutdown(path)
        finally:
            # Stop the server instance; TornadoWeb handles ref counts per port
            _state.mgr.tornado_web.stop()

    requests = from_web()
    requests_by_url = partition(requests, requests.url)

    responses = {}
    for url, handler in HttpAdaptorManager.instance().handlers.items():
        if isinstance(handler, WiringNodeClass):
            logger.info("Adding handler: [%s] %s", url, handler.signature.signature)
            if handler.signature.time_series_inputs["request"].matches_type(TS[HttpRequest]):
                responses[url] = map_(handler, request=requests_by_url[url], __label__=url)
            elif handler.signature.time_series_inputs["request"].matches_type(TSD[int, TS[HttpRequest]]):
                responses[url] = handler(request=requests_by_url[url])
        elif handler is None:
            logger.info("Pre-wired handler: [%s]", url)
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
