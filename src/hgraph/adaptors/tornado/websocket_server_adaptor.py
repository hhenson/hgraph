import asyncio
from dataclasses import dataclass
from typing import Callable

import tornado.websocket
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
    adaptor_impl,
    merge,
    register_service,
    TSB,
    TS_SCHEMA,
    TIME_SERIES_TYPE,
    HgTSBTypeMetaData,
    TimeSeriesSchema,
    combine,
    REMOVE,
)
from hgraph.adaptors.tornado._tornado_web import TornadoWeb


@dataclass(frozen=True)
class WebSocketConnectRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str, ...] = ()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()


class WebSocketServerRequest(TimeSeriesSchema):
    connect_request: TS[WebSocketConnectRequest]
    messages: TS[tuple[bytes, ...]]


class WebSocketClientRequest(TimeSeriesSchema):
    connect_request: TS[WebSocketConnectRequest]
    message: TS[bytes]


class WebSocketResponse(TimeSeriesSchema):
    connect_response: TS[bool]
    message: TS[bytes]


class WebSocketAdaptorManager:
    handlers: dict[str, Callable | str]

    def __init__(self):
        self.handlers = {}
        self.requests = {}
        self.message_handlers = {}

    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
        return cls._instance

    def set_queues(self, connect_queue, message_queue):
        self.connect_queue = connect_queue
        self.message_queue = message_queue

    def start(self, port):
        self.tornado_web = TornadoWeb.instance(port)

        for path in self.handlers.keys():
            self.tornado_web.add_handler(path, WebSocketHandler, {"path": path, "mgr": self})

        self.tornado_web.start()

    def stop(self):
        self.tornado_web.stop()

    def add_handler(self, path, handler):
        self.handlers[path] = handler

    def add_request(self, request_id, request, message_handler):
        try:
            future = asyncio.Future()
        except Exception as e:
            print(f"Error creating future: {e}")
            raise e
        self.requests[request_id] = future
        self.message_handlers[request_id] = message_handler
        self.connect_queue({request_id: request})
        return future

    def remove_message_handler(self, request_id):
        del self.message_handlers[request_id]
        self.connect_queue({request_id: REMOVE})
        self.message_queue({request_id: REMOVE})

    def complete_request(self, request_id, response):
        if r := response.get("connect_response"):
            self.requests[request_id].set_result(
                (r, lambda m: self.message_queue({request_id: m}), lambda: self.remove_message_handler(request_id))
            )
            print(f"Completed websocket open request {request_id} with response {response}")
        if m := response.get("message"):
            if h := self.message_handlers.get(request_id):
                h(m)


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def initialize(self, path, mgr: WebSocketAdaptorManager):
        self.path = path
        self.mgr = mgr

    async def open(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response, enqueue, close = await self.mgr.add_request(
            request_id,
            WebSocketConnectRequest(
                url=self.path,
                url_parsed_args=args,
                headers=self.request.headers,
                cookies=frozendict({k: frozendict({'value': v.value, **{p: w for p, w in v.items()}}) for k, v in self.request.cookies.items()}),
            ),
            lambda m: self.write_message(m, binary=True),
        )

        if response:
            self.enqueue_message = enqueue
            self.close = close
        else:
            self.close()

    def on_message(self, message):
        self.enqueue_message(message if type(message) is bytes else message.encode())

    def on_close(self):
        self.close()


def websocket_server_handler(fn: Callable = None, *, url: str):
    if fn is None:
        return lambda fn: websocket_server_handler(fn, url=url)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "request" in fn.signature.time_series_inputs.keys(), "Websocket graph must have an input named 'request'"
    assert fn.signature.time_series_inputs["request"].matches_type(
        TSB[WebSocketServerRequest]
    ) or fn.signature.time_series_inputs["request"].matches_type(TSD[int, TSB[WebSocketServerRequest]]), (
        "WebSocket graph must have a single input named 'request' of type TSB[WebSocketServerRequest] or TSD[int,"
        " TSB[WebSocketServerRequest]]"
    )

    output_type = fn.signature.output_type

    assert output_type.matches_type(TSB[WebSocketResponse]) or output_type.matches_type(
        TSD[int, TSB[WebSocketResponse]]
    ), "WebSocket graph must have a single output of type TSB[WebSocketResponse] or TSD[int, TSB[WebSocketResponse]]"

    mgr = WebSocketAdaptorManager.instance()
    # this makes the handler to be auto-wired in the http_server_adaptor
    mgr.add_handler(url, fn)

    @graph
    def websocket_server_handler_graph(**inputs: TSB[TS_SCHEMA]) -> TIME_SERIES_TYPE:
        # if however this is wired into the graph explicitly, it will be used instead of the auto-wiring the handler
        mgr.add_handler(url, None)  # prevent auto-wiring

        requests = websocket_server_adaptor.to_graph(path=url, __no_ts_inputs__=True)
        if fn.signature.time_series_inputs["request"].matches_type(TSB[WebSocketServerRequest]):
            if inputs.as_dict():
                responses = map_(lambda r, i: fn(request=r, **i.as_dict()), requests, inputs)
            else:
                responses = map_(lambda r: fn(request=r), requests)
        else:
            responses = fn(request=requests, **inputs)

        if (
            isinstance(responses.output_type, HgTSBTypeMetaData)
            and "response" in responses.output_type.bundle_schema_tp.meta_data_schema
        ):
            websocket_server_adaptor.from_graph(responses.response, path=url)
            return responses
        else:
            websocket_server_adaptor.from_graph(responses, path=url)
            return combine()

    return websocket_server_handler_graph


@adaptor
def websocket_server_adaptor(
    response: TSD[int, TSB[WebSocketResponse]], path: str
) -> TSD[int, TSB[WebSocketServerRequest]]: ...


@adaptor_impl(interfaces=(websocket_server_adaptor, websocket_server_adaptor))
def websocket_server_adaptor_helper(path: str, port: int):
    register_service("websocket_server_adaptor", websocket_server_adaptor_impl, port=port)


@adaptor_impl(interfaces=())
def websocket_server_adaptor_impl(path: str, port: int):
    from hgraph import WiringNodeClass
    from hgraph import WiringGraphContext

    @push_queue(TSD[int, TS[WebSocketConnectRequest]])
    def connections_from_web(sender, path: str = "tornado_websocket_server_adaptor", elide: bool = True) -> TSD[int, TS[WebSocketConnectRequest]]:
        GlobalState.instance()[f"websocket_server_adaptor://{path}/connect_queue"] = sender
        return None

    @push_queue(TSD[int, TS[tuple[bytes, ...]]])
    def messages_from_web(sender, path: str = "tornado_websocket_server_adaptor", batch: bool = True) -> TSD[int, TS[tuple[bytes, ...]]]:
        GlobalState.instance()[f"websocket_server_adaptor://{path}/message_queue"] = sender
        return None

    @graph
    def from_web(path: str) -> TSD[int, TSB[WebSocketServerRequest]]:
        requests = connections_from_web(path=path)
        messages = messages_from_web(path=path)
        return map_(lambda r, m: combine[TSB[WebSocketServerRequest]](connect_request=r, messages=m), requests, messages)

    @sink_node
    def to_web(
        responses: TSD[int, TSB[WebSocketResponse]],
        port: int,
        path: str = "tornado_websocket_server_adaptor",
        _state: STATE = None,
    ):
        for response_id, response in responses.modified_items():
            TornadoWeb.get_loop().add_callback(_state.mgr.complete_request, response_id, response.delta_value)

    @to_web.start
    def to_web_start(port: int, path: str, _state: STATE):
        _state.mgr = WebSocketAdaptorManager.instance()
        _state.mgr.set_queues(
            connect_queue=GlobalState.instance()[f"websocket_server_adaptor://{path}/connect_queue"],
            message_queue=GlobalState.instance()[f"websocket_server_adaptor://{path}/message_queue"],
            )
        _state.mgr.start(port)

    @to_web.stop
    def to_web_stop(_state: STATE):
        _state.mgr.tornado_web.stop()

    requests = from_web(path=path)
    requests_by_url = partition(requests, requests.connect_request.url)

    responses = {}
    for url, handler in WebSocketAdaptorManager.instance().handlers.items():
        if isinstance(handler, WiringNodeClass):
            if handler.signature.time_series_inputs["request"].matches_type(TSB[WebSocketServerRequest]):
                responses[url] = map_(handler, request=requests_by_url[url])
            elif handler.signature.time_series_inputs["request"].matches_type(TSD[int, TSB[WebSocketServerRequest]]):
                responses[url] = handler(request=requests_by_url[url])
        elif handler is None:
            pass
        else:
            raise ValueError(f"Invalid handler type for the websocket_ adaptor: {handler}")

    adaptors_dedup = set()
    adaptors = set()
    for handler_path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(
        websocket_server_adaptor
    ):
        assert type_map == {}, "Websocket adaptor does not support type generics"
        if (handler_path, receive) in adaptors_dedup:
            raise ValueError(f"Duplicate websocket_ adaptor client for handler_path {handler_path}: only one client is allowed")
        adaptors_dedup.add((handler_path, receive))
        adaptors.add(handler_path.replace("/from_graph", "").replace("/to_graph", ""))

    for handler_path in adaptors:
        url = websocket_server_adaptor.path_from_full_path(handler_path)

        mgr = WebSocketAdaptorManager.instance()
        mgr.add_handler(url, None)

        responses[url] = websocket_server_adaptor.wire_impl_inputs_stub(handler_path).response
        websocket_server_adaptor.wire_impl_out_stub(handler_path, requests_by_url[url])

    to_web(merge(*responses.values(), disjoint=True), port, path=path)
