"""Tornado WebSocket server adaptor and graph handler support."""

import asyncio
import inspect
import threading
import weakref
from dataclasses import dataclass
from typing import Generic, TypeVar

import tornado.websocket
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    REMOVE_IF_EXISTS,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    adaptor,
    adaptor_impl,
    combine,
    feedback,
    from_graph,
    graph,
    map_,
    merge,
    partition,
    push_queue,
    register_adaptor,
    sink_node,
    to_graph,
)
from hgraph._wiring import _GraphFn, _PyNode
from hgraph._wiring._core import _current_wiring

from ._tornado_web import BaseHandler, TornadoWeb


STR_OR_BYTES = TypeVar("STR_OR_BYTES", bytes, str)


@dataclass(frozen=True)
class WebSocketConnectRequest(CompoundScalar):
    url: str
    url_parsed_args: tuple[str, ...] = ()
    headers: dict[str, str] = frozendict()
    cookies: dict[str, dict[str, object]] = frozendict()
    auth: object = None


class WebSocketServerRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    messages: TS[tuple[STR_OR_BYTES, ...]]


class WebSocketClientRequest(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_request: TS[WebSocketConnectRequest]
    message: TS[STR_OR_BYTES]


class WebSocketResponse(TimeSeriesSchema, Generic[STR_OR_BYTES]):
    connect_response: TS[bool]
    message: TS[STR_OR_BYTES]


class WebSocketAdaptorManager:
    """Bridge one message type and port between Tornado and graph queues."""

    _instances = {}
    _instances_lock = threading.Lock()

    @classmethod
    def instance(cls, port: int, message_type: type) -> "WebSocketAdaptorManager":
        key = (port, message_type)
        with cls._instances_lock:
            manager = cls._instances.get(key)
            if manager is None:
                manager = cls(port, message_type)
                cls._instances[key] = manager
            return manager

    def __init__(self, port: int, message_type: type):
        self._web = TornadoWeb.instance(port)
        self._message_type = message_type
        self._queues = {}
        self._registered_paths = set()
        self._requests = {}
        self._pending_connect = {}
        self._next_request_id = 1

    def set_queues(self, path, connect_sender, message_sender) -> None:
        self._queues[path] = (connect_sender, message_sender)
        for request_id, request in self._pending_connect.pop(path, ()):
            if request_id in self._requests:
                connect_sender({request_id: request})

    @property
    def binary(self) -> bool:
        return self._message_type is bytes

    def start(self, path: str) -> None:
        self.register(path)
        self._web.start()

    def start_routes(self, paths, connect_sender, message_sender) -> None:
        for path in paths:
            self.register(path)
            self.set_queues(path, connect_sender, message_sender)
        self._web.start()

    def stop_routes(self, paths) -> None:
        paths = set(paths)
        completed = threading.Event()

        def cleanup():
            try:
                for path in paths:
                    self._queues.pop(path, None)
                    self._pending_connect.pop(path, None)
                for request_id, pending in tuple(self._requests.items()):
                    request_path, future, _ = pending
                    if request_path in paths:
                        if not future.done():
                            future.cancel()
                        self._requests.pop(request_id, None)
            finally:
                completed.set()

        TornadoWeb.get_loop().add_callback(cleanup)
        if not completed.wait(timeout=5.0):
            raise RuntimeError("WebSocket routes did not stop")
        self._web.stop()

    def register(self, path: str) -> None:
        if path not in self._registered_paths:
            self._web.add_handler(
                path,
                WebSocketHandler,
                {"path": path, "manager": self},
            )
            self._registered_paths.add(path)

    def stop(self, path: str) -> None:
        completed = threading.Event()

        def cleanup():
            try:
                self._queues.pop(path, None)
                for request_id, pending in tuple(self._requests.items()):
                    request_path, future, _ = pending
                    if request_path != path:
                        continue
                    if not future.done():
                        future.cancel()
                    self._requests.pop(request_id, None)
                self._pending_connect.pop(path, None)
            finally:
                completed.set()

        TornadoWeb.get_loop().add_callback(cleanup)
        if not completed.wait(timeout=5.0):
            raise RuntimeError(f"WebSocket route {path!r} did not stop")
        self._web.stop()

    def add_request(self, path, request, message_handler):
        senders = self._queues.get(path)
        request_id = self._next_request_id
        self._next_request_id += 1
        future = asyncio.get_running_loop().create_future()
        self._requests[request_id] = (path, future, message_handler)
        if senders is None:
            # A sibling route may already have started the shared server.
            # Preserve the accepted upstream behaviour by holding the open
            # request until this route's graph queues enter their start phase.
            self._pending_connect.setdefault(path, []).append((request_id, request))
        else:
            senders[0]({request_id: request})
        return request_id, future

    def add_message(self, request_id: int, message) -> None:
        pending = self._requests.get(request_id)
        if pending is None:
            return
        normalized = message
        if self._message_type is bytes and isinstance(message, str):
            normalized = message.encode()
        elif self._message_type is str and isinstance(message, bytes):
            normalized = message.decode()
        self._queues[pending[0]][1]({request_id: (normalized,)})

    def complete_request(self, request_id: int, response) -> None:
        pending = self._requests.get(request_id)
        if pending is None:
            return
        _, future, message_handler = pending
        if "connect_response" in response and not future.done():
            future.set_result(bool(response["connect_response"]))
        if "message" in response:
            TornadoWeb.get_loop().add_callback(message_handler, response["message"])

    def remove_request(self, request_id: int) -> None:
        pending = self._requests.pop(request_id, None)
        if pending is None:
            return
        path, future, _ = pending
        if not future.done():
            future.cancel()
        queued = self._pending_connect.get(path)
        if queued is not None:
            self._pending_connect[path] = [item for item in queued if item[0] != request_id]
            if not self._pending_connect[path]:
                self._pending_connect.pop(path, None)
        senders = self._queues.get(path)
        if senders is not None:
            # A rejected connection never creates a message-series key, and
            # shutdown may race a sparse keyed input.  Teardown is therefore
            # deliberately idempotent for both streams.
            senders[0]({request_id: REMOVE_IF_EXISTS})
            senders[1]({request_id: REMOVE_IF_EXISTS})


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def initialize(self, path: str, manager: WebSocketAdaptorManager) -> None:
        self._path = path
        self._manager = manager
        self._request_id = None
        self._accepted = False

    async def prepare(self) -> None:
        await BaseHandler.prepare(self)

    async def open(self, *args) -> None:
        request = WebSocketConnectRequest(
            url=self._path,
            url_parsed_args=tuple(args),
            headers=frozendict(self.request.headers.items()),
            cookies=frozendict(
                (
                    key,
                    frozendict({"value": morsel.value, **dict(morsel.items())}),
                )
                for key, morsel in self.request.cookies.items()
            ),
            auth=getattr(self, "current_user", None),
        )
        self._request_id, response = self._manager.add_request(
            self._path,
            request,
            lambda message: self.write_message(
                message,
                binary=self._manager.binary,
            ),
        )
        try:
            self._accepted = await response
        except asyncio.CancelledError:
            self.close()
            return
        if not self._accepted:
            self.close()

    def on_message(self, message) -> None:
        if self._accepted and self._request_id is not None:
            self._manager.add_message(self._request_id, message)

    def on_close(self) -> None:
        if self._request_id is not None:
            self._manager.remove_request(self._request_id)
            self._request_id = None


@adaptor
def websocket_server_adaptor(
    response: TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]],
    path: str,
) -> TSD[int, TSB[WebSocketServerRequest[STR_OR_BYTES]]]:
    """Expose a typed graph request/response stream as a WebSocket route."""


class _WebSocketServerHandler:
    def __init__(self, fn, url: str):
        self._fn = fn if isinstance(fn, (_GraphFn, _PyNode)) else graph(fn)
        self.url = url
        self.__name__ = getattr(fn, "__name__", "websocket_server_handler")

        target = getattr(fn, "fn", fn)
        signature = inspect.signature(target, eval_str=True)
        request = signature.parameters.get("request")
        if request is None:
            raise TypeError("WebSocket handler requires a 'request' time-series input")

        self._single = None
        self.message_type = None
        for message_type in (str, bytes):
            single = TSB[WebSocketServerRequest[message_type]]
            batch = TSD[int, single]
            if request.annotation == single:
                self._single = True
                self.message_type = message_type
                expected_output = TSB[WebSocketResponse[message_type]]
                break
            if request.annotation == batch:
                self._single = False
                self.message_type = message_type
                expected_output = TSD[int, TSB[WebSocketResponse[message_type]]]
                break
        if self.message_type is None:
            raise TypeError(
                "WebSocket handler request must be TSB[WebSocketServerRequest[str|bytes]] "
                "or its keyed TSD form"
            )
        if signature.return_annotation != expected_output:
            raise TypeError(
                f"WebSocket handler output must be {expected_output!r} with the same message type"
            )

        from hgraph._wiring._operator import _is_hidden_node_parameter

        # Canonical predicate: covers the whole injectable family incl.
        # typed STATE[T] expressions (review catch on this PR).
        parameters = tuple(
            parameter
            for name, parameter in signature.parameters.items()
            if name != "request" and not _is_hidden_node_parameter(parameter)
        )
        self.__signature__ = signature.replace(parameters=parameters)
        self.auto_wire = all(
            parameter.default is not inspect.Parameter.empty
            for parameter in parameters
        )
        self._wired = weakref.WeakKeyDictionary()

    def __call__(self, *args, **kwargs):
        wiring = _current_wiring()
        if not args and not kwargs and wiring in self._wired:
            return self._wired[wiring]
        _ensure_websocket_route_registered(
            wiring, self.url, self.message_type)
        bound = self.__signature__.bind(*args, **kwargs)
        bound.apply_defaults()
        response_type = TSD[int, TSB[WebSocketResponse[self.message_type]]]
        interface = websocket_server_adaptor[STR_OR_BYTES:self.message_type]
        response_feedback = feedback(response_type)
        requests = interface(response_feedback(), path=self.url)
        if self._single:
            responses = map_(self._fn, requests, *bound.args, **bound.kwargs)
        else:
            responses = self._fn(request=requests, **bound.arguments)
        response_feedback(responses)
        if not args and not kwargs:
            self._wired[wiring] = responses
        return responses


_WEBSOCKET_SERVER_HANDLERS = {}
_WEBSOCKET_SERVER_REGISTRATIONS = weakref.WeakKeyDictionary()


def _ensure_websocket_route_registered(wiring, path, message_type):
    del wiring, path, message_type


def _wire_websocket_message_type(port, message_type, entries):
    interface = websocket_server_adaptor[STR_OR_BYTES:message_type]
    request_bundle = TSB[WebSocketServerRequest[message_type]]
    response_bundle = TSB[WebSocketResponse[message_type]]
    responses_type = TSD[int, response_bundle]
    message_ts = TS[tuple[message_type, ...]]
    routes = {
        base: interface.path_from_full_path(base)
        for base in entries
    }
    manager = WebSocketAdaptorManager.instance(port, message_type)
    sender_refs = {}

    @push_queue(TSD[int, TS[WebSocketConnectRequest]])
    def connections_from_web(sender) -> None:
        sender_refs["connect"] = sender

    @push_queue(TSD[int, message_ts])
    def messages_from_web(sender) -> None:
        sender_refs["message"] = sender

    @graph
    def make_request(
        connect_request: TS[WebSocketConnectRequest],
        messages: message_ts,
    ) -> request_bundle:
        return combine[request_bundle](
            connect_request=connect_request,
            messages=messages,
        )

    @graph
    def connection_url(request: TS[WebSocketConnectRequest]) -> TS[str]:
        return request.url

    @sink_node
    def to_web(responses: responses_type) -> None:
        loop = TornadoWeb.get_loop()
        for request_id, response in responses.modified_items():
            loop.add_callback(
                manager.complete_request, request_id, response.delta_value)

    @to_web.start
    def to_web_start() -> None:
        manager.start_routes(
            tuple(routes.values()),
            sender_refs["connect"], sender_refs["message"])

    @to_web.stop
    def to_web_stop() -> None:
        try:
            manager.stop_routes(tuple(routes.values()))
        finally:
            sender_refs.clear()

    connections = connections_from_web()
    messages = messages_from_web()
    urls = map_(connection_url, connections)
    connections_by_url = partition(connections, urls)
    messages_by_url = partition(messages, urls)
    responses = []
    for base, route in routes.items():
        responses.append(from_graph(interface, path=base))
        to_graph(
            interface,
            map_(
                make_request,
                connections_by_url[route], messages_by_url[route]),
            path=base)
    if responses:
        to_web(merge(*responses, disjoint=True))


@adaptor_impl(interfaces=())
def _websocket_server_catch_all(path: str, port: int = 80) -> None:
    from hgraph import WiringGraphContext
    from hgraph.reflection import resolved_type

    clients = WiringGraphContext.instance().registered_service_clients(
        websocket_server_adaptor)
    endpoints = set()
    grouped = {str: [], bytes: []}
    seen = {str: set(), bytes: set()}
    for endpoint, type_map, _node, receive in clients:
        if (endpoint, receive) in endpoints:
            raise ValueError(
                f"duplicate WebSocket adaptor client for {endpoint!r} and direction {receive}")
        endpoints.add((endpoint, receive))
        message_type = resolved_type(type_map[STR_OR_BYTES])
        base = endpoint.removesuffix("/from_graph").removesuffix("/to_graph")
        if base not in seen[message_type]:
            grouped[message_type].append(base)
            seen[message_type].add(base)
    handler_priority = {
        route: index for index, route in enumerate(_WEBSOCKET_SERVER_HANDLERS)
    }
    for message_type, entries in grouped.items():
        if entries:
            interface = websocket_server_adaptor[STR_OR_BYTES:message_type]
            entries.sort(key=lambda base: handler_priority.get(
                interface.path_from_full_path(base), len(handler_priority)))
            _wire_websocket_message_type(port, message_type, entries)

def websocket_server_handler(fn=None, *, url: str):
    """Declare a typed WebSocket route handled by a graph or Python node."""
    if fn is None:
        return lambda decorated: websocket_server_handler(decorated, url=url)
    handler = _WebSocketServerHandler(fn, url)
    _WEBSOCKET_SERVER_HANDLERS[url] = handler
    return handler


class _WebSocketServerAdaptorHelperRegistration:
    """Compatibility marker for explicitly wired WebSocket handler graphs.

    Specialized graph-side clients are discovered by the shared catch-all, so
    the marker must not compete with it for ownership in the C++ registry.
    """

    __name__ = "websocket_server_adaptor_helper"

    @staticmethod
    def _register_adaptor(path, *, resolution_dict=None, port=80):
        del path, port
        if resolution_dict:
            raise TypeError("websocket_server_adaptor_helper resolves from its handlers")


websocket_server_adaptor_helper = _WebSocketServerAdaptorHelperRegistration()


def register_websocket_server_adaptor(port: int) -> None:
    """Register all declared WebSocket routes on ``port``."""
    wiring = _current_wiring()
    registration = _WEBSOCKET_SERVER_REGISTRATIONS.setdefault(wiring, port)
    if registration != port:
        raise ValueError("one wiring graph cannot register the WebSocket server on two ports")
    register_adaptor(None, websocket_server_adaptor_helper, port=port)
    register_adaptor(
        "websocket_server_adaptor", _websocket_server_catch_all, port=port)
    for _url, handler in tuple(_WEBSOCKET_SERVER_HANDLERS.items()):
        if handler.auto_wire:
            handler()


class _WebSocketServerAdaptorImplementation:
    """Public registration token for the shared typed server implementation."""

    __name__ = "websocket_server_adaptor_impl"

    @staticmethod
    def _register_adaptor(path, *, resolution_dict=None, port=80):
        del path
        if resolution_dict:
            raise TypeError(
                "websocket_server_adaptor_impl resolves message types from its handlers"
            )
        register_websocket_server_adaptor(port)


websocket_server_adaptor_impl = _WebSocketServerAdaptorImplementation()


__all__ = (
    "WebSocketClientRequest",
    "WebSocketConnectRequest",
    "WebSocketResponse",
    "WebSocketServerRequest",
    "register_websocket_server_adaptor",
    "websocket_server_adaptor",
    "websocket_server_adaptor_helper",
    "websocket_server_adaptor_impl",
    "websocket_server_handler",
)
