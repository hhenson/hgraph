import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Generic, Type, TypeVar

import tornado.websocket
from frozendict import frozendict

from hgraph import (
    AUTO_RESOLVE,
    REMOVE,
    STATE,
    TIME_SERIES_TYPE,
    TS,
    TS_SCHEMA,
    TSB,
    TSD,
    CompoundScalar,
    GlobalState,
    HgTSBTypeMetaData,
    HgTypeMetaData,
    TimeSeriesSchema,
    adaptor,
    adaptor_impl,
    combine,
    graph,
    map_,
    merge,
    partition,
    push_queue,
    register_service,
    sink_node,
)
from hgraph.adaptors.tornado._tornado_web import TornadoWeb

logger = logging.getLogger("websocket_server_adaptor")


@dataclass(frozen=True)
class WebSocketConnectRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str, ...] = ()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()


STR_OR_BYTES = TypeVar("STR_OR_BYTES", bytes, str)


@dataclass(frozen=True)
class WebSocketServerRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    messages: TS[tuple[STR_OR_BYTES, ...]]


@dataclass(frozen=True)
class WebSocketClientRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    message: TS[STR_OR_BYTES]


@dataclass(frozen=True)
class WebSocketResponse(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_response: TS[bool]
    message: TS[STR_OR_BYTES]


class WebSocketAdaptorManager:
    handlers: dict[str, Callable | str]

    def __init__(self, binary):
        self.handlers = {}
        self.requests = {}
        self.message_handlers = {}
        self.binary = binary

    @classmethod
    def instance(cls, tp):
        if not hasattr(cls, "_instance"):
            cls._instance = {}
        if tp not in cls._instance:
            cls._instance[tp] = cls(tp == bytes)
        return cls._instance[tp]

    def set_queues(self, connect_queue, message_queue):
        self.connect_queue = connect_queue
        self.message_queue = message_queue

    def start(self, port):
        self.tornado_web = TornadoWeb.instance(port)

        for path in self.handlers.keys():
            self.tornado_web.add_handler(path, WebSocketHandler, {"path": path, "binary": self.binary, "mgr": self})

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
    def initialize(self, path, binary, mgr: WebSocketAdaptorManager):
        self.path = path
        self.mgr = mgr
        self.binary = binary

    async def open(self, *args):
        request_obj = object()
        request_id = id(request_obj)

        response, enqueue, close = await self.mgr.add_request(
            request_id,
            WebSocketConnectRequest(
                url=self.path,
                url_parsed_args=args,
                headers=self.request.headers,
                cookies=frozendict({
                    k: frozendict({"value": v.value, **{p: w for p, w in v.items()}})
                    for k, v in self.request.cookies.items()
                }),
            ),
            lambda m: self.write_message(m, binary=self.binary),
        )

        if response:
            self.enqueue_message = enqueue
            self.close = close
        else:
            self.close()

    def on_message(self, message):
        if self.binary:
            self.enqueue_message(message if type(message) is bytes else message.encode())
        else:
            self.enqueue_message(message if type(message) is str else message.decode())

    def on_close(self):
        self.close()


def websocket_server_handler(fn: Callable = None, *, url: str):
    if fn is None:
        return lambda fn: websocket_server_handler(fn, url=url)

    from hgraph import WiringNodeClass

    if not isinstance(fn, WiringNodeClass):
        fn = graph(fn)

    assert "request" in fn.signature.time_series_inputs.keys(), "Websocket graph must have an input named 'request'"

    single_request_type = HgTypeMetaData.parse_type(TSB[WebSocketServerRequest[STR_OR_BYTES]])
    multi_request_type = HgTypeMetaData.parse_type(TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]])

    if single_request_type.matches(fn.signature.time_series_inputs["request"]):
        is_single = True
        resolution = {}
        single_request_type.build_resolution_dict(resolution, fn.signature.time_series_inputs["request"])
        assert (
            STR_OR_BYTES in resolution
        ), f"STR_OR_BYTES is expected in the resolution of the request type, but got {resolution.keys()}"
        is_binary = resolution[STR_OR_BYTES].matches_type(bytes)
    elif multi_request_type.matches(fn.signature.time_series_inputs["request"]):
        is_single = False
        resolution = {}
        multi_request_type.build_resolution_dict(resolution, fn.signature.time_series_inputs["request"])
        is_binary = resolution[STR_OR_BYTES].matches_type(bytes)
    else:
        assert False, (
            "WebSocket graph must have a single input named 'request' of type TSB[WebSocketServerRequest] or TSD[int,"
            " TSB[WebSocketServerRequest]]"
        )

    output_type = fn.signature.output_type

    single_response_type = HgTypeMetaData.parse_type(TSB[WebSocketResponse[STR_OR_BYTES]])
    multi_response_type = HgTypeMetaData.parse_type(TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]])

    if single_response_type.matches(output_type):
        resolution = {}
        single_response_type.build_resolution_dict(resolution, output_type)
        assert is_binary == resolution[STR_OR_BYTES].matches_type(bytes), (
            "WebSocket graph must have a single output of type TSB[WebSocketResponse] with the same str/binary type as"
            " the input"
        )
        assert is_single, (
            "WebSocket graph must have a single output of type TSB[WebSocketResponse] when the input is"
            " TSB[WebSocketServerRequest]"
        )
    elif multi_response_type.matches(output_type):
        resolution = {}
        multi_response_type.build_resolution_dict(resolution, output_type)
        assert is_binary == resolution[STR_OR_BYTES].matches_type(bytes), (
            "WebSocket graph must have a single output of type TSD[int, TSB[WebSocketResponse]] with the same"
            " str/binary type as the input"
        )
        assert not is_single, (
            "WebSocket graph must have a single output of type TSD[int, TSB[WebSocketResponse]] when the input is"
            " TSD[int, TSB[WebSocketServerRequest]]"
        )
    else:
        assert False, (
            "WebSocket graph must have a single output of type TSB[WebSocketResponse] or TSD[int,"
            " TSB[WebSocketResponse]]"
        )

    msg_type = bytes if is_binary else str
    mgr = WebSocketAdaptorManager.instance(msg_type)
    # this makes the handler to be auto-wired in the http_server_adaptor
    mgr.add_handler(url, fn)

    @graph
    def websocket_server_handler_graph(**inputs: TSB[TS_SCHEMA]) -> TIME_SERIES_TYPE:
        # if however this is wired into the graph explicitly, it will be used instead of the auto-wiring the handler
        mgr.add_handler(url, None)  # prevent auto-wiring

        requests = websocket_server_adaptor[STR_OR_BYTES:msg_type].to_graph(path=url, __no_ts_inputs__=True)
        if fn.signature.time_series_inputs["request"].matches_type(TSB[WebSocketServerRequest[msg_type]]):
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
            websocket_server_adaptor[STR_OR_BYTES:msg_type].from_graph(responses.response, path=url)
            return responses
        else:
            websocket_server_adaptor[STR_OR_BYTES:msg_type].from_graph(responses, path=url)
            return combine()

    return websocket_server_handler_graph


@adaptor
def websocket_server_adaptor(
    response: TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]], path: str
) -> TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]]: ...


@adaptor_impl(interfaces=(websocket_server_adaptor, websocket_server_adaptor))
def websocket_server_adaptor_helper(path: str, port: int):
    register_service("websocket_server_adaptor", websocket_server_adaptor_impl, port=port)


@adaptor_impl(interfaces=())
def websocket_server_adaptor_impl(path: str, port: int):
    from hgraph import WiringGraphContext, WiringNodeClass

    @push_queue(TSD[int, TS[WebSocketConnectRequest]])
    def connections_from_web(
        sender, path: str = "tornado_websocket_server_adaptor", elide: bool = True
    ) -> TSD[int, TS[WebSocketConnectRequest]]:
        GlobalState.instance()[f"websocket_server_adaptor://{path}/connect_queue"] = sender
        return None

    @push_queue(TSD[int, TS[tuple[STR_OR_BYTES, ...]]])
    def messages_from_web(
        sender, path: str = "tornado_websocket_server_adaptor", batch: bool = True
    ) -> TSD[int, TS[tuple[bytes, ...]]]:
        GlobalState.instance()[f"websocket_server_adaptor://{path}/message_queue"] = sender
        return None

    @graph
    def from_web(
        path: str, _tp: Type[STR_OR_BYTES] = AUTO_RESOLVE
    ) -> TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]]:
        path = f"{path}[{_tp.__name__.lower()}]"
        requests = connections_from_web(path=path)
        messages = messages_from_web[STR_OR_BYTES:_tp](path=path)
        return map_(
            lambda r, m: combine[TSB[WebSocketServerRequest[_tp]]](connect_request=r, messages=m), requests, messages
        )

    @sink_node
    def to_web(
        responses: TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]],
        port: int,
        path: str = "tornado_websocket_server_adaptor",
        _tp: Type[STR_OR_BYTES] = AUTO_RESOLVE,
        _state: STATE = None,
    ):
        for response_id, response in responses.modified_items():
            TornadoWeb.get_loop().add_callback(_state.mgr.complete_request, response_id, response.delta_value)

    @to_web.start
    def to_web_start(port: int, path: str, _tp: Type[STR_OR_BYTES] = AUTO_RESOLVE, _state: STATE = None):
        _state.mgr = WebSocketAdaptorManager.instance(_tp)
        path = f"{path}[{_tp.__name__.lower()}]"
        _state.mgr.set_queues(
            connect_queue=GlobalState.instance()[f"websocket_server_adaptor://{path}/connect_queue"],
            message_queue=GlobalState.instance()[f"websocket_server_adaptor://{path}/message_queue"],
        )
        _state.mgr.start(port)

    @to_web.stop
    def to_web_stop(_state: STATE):
        _state.mgr.tornado_web.stop()

    adaptors_dedup = set()
    for msg_type in (str, bytes):
        if WebSocketAdaptorManager.instance(msg_type).handlers:
            requests = from_web[STR_OR_BYTES:msg_type](path=path)
            requests_by_url = partition(requests, requests.connect_request.url)

            responses = {}
            for url, handler in WebSocketAdaptorManager.instance(msg_type).handlers.items():
                if isinstance(handler, WiringNodeClass):
                    logger.info("Adding WS handler: [%s] %s", url, handler.signature.signature)
                    if handler.signature.time_series_inputs["request"].matches_type(TSB[WebSocketServerRequest]):
                        responses[url] = map_(handler, request=requests_by_url[url])
                    elif handler.signature.time_series_inputs["request"].matches_type(
                        TSD[int, TSB[WebSocketServerRequest]]
                    ):
                        responses[url] = handler(request=requests_by_url[url])
                elif handler is None:
                    logger.info("Pre-wired WS handler: [%s]", url)
                else:
                    raise ValueError(f"Invalid handler type for the websocket_ adaptor: {handler}")

            adaptors = set()
            for handler_path, type_map, node, receive in WiringGraphContext.__stack__[0].registered_service_clients(
                websocket_server_adaptor
            ):
                logger.info(
                    f"Adding WS adaptor: {handler_path} for type {type_map[STR_OR_BYTES]} when msg_type is {msg_type}"
                )
                if type_map[STR_OR_BYTES].py_type != msg_type:
                    continue
                if (handler_path, receive) in adaptors_dedup:
                    raise ValueError(
                        f"Duplicate websocket_ adaptor client for handler_path {handler_path}: only one client is"
                        " allowed"
                    )
                adaptors_dedup.add((handler_path, receive))
                adaptors.add(handler_path.replace("/from_graph", "").replace("/to_graph", ""))

            for handler_path in adaptors:
                url = websocket_server_adaptor.path_from_full_path(handler_path)

                mgr = WebSocketAdaptorManager.instance(msg_type)
                mgr.add_handler(url, None)  # prevent auto-wiring the handler

                responses[url] = (
                    websocket_server_adaptor[STR_OR_BYTES:msg_type].wire_impl_inputs_stub(handler_path).response
                )
                websocket_server_adaptor[STR_OR_BYTES:msg_type].wire_impl_out_stub(handler_path, requests_by_url[url])

            to_web(merge(*responses.values(), disjoint=True), port, path=path)
