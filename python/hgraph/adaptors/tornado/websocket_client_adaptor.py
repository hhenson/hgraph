"""Tornado WebSocket client implemented as a generic service adaptor."""

import asyncio
from collections import defaultdict, deque
import threading

from tornado import httpclient
from tornado.websocket import websocket_connect

from hgraph import (
    STATE,
    TSB,
    TSD,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph._wiring._services import _AdaptorImplGroup

from ._tornado_web import TornadoWeb
from .websocket_server_adaptor import (
    STR_OR_BYTES,
    WebSocketClientRequest,
    WebSocketResponse,
)


@service_adaptor
def websocket_client_adaptor(
    request: TSB[WebSocketClientRequest[STR_OR_BYTES]],
    path: str = "websocket_client",
) -> TSB[WebSocketResponse[STR_OR_BYTES]]:
    """Open a typed WebSocket connection and exchange messages."""


_CLIENT_IMPLEMENTATIONS = {}


def _client_implementation(message_type: type):
    implementation = _CLIENT_IMPLEMENTATIONS.get(message_type)
    if implementation is not None:
        return implementation

    interface = websocket_client_adaptor[STR_OR_BYTES:message_type]
    request_bundle = TSB[WebSocketClientRequest[message_type]]
    response_bundle = TSB[WebSocketResponse[message_type]]
    requests_type = TSD[int, request_bundle]
    responses_type = TSD[int, response_bundle]
    binary = message_type is bytes

    @service_adaptor_impl(interfaces=interface)
    def websocket_client_adaptor_impl(
        request: requests_type,
        path: str = "websocket_client",
    ) -> responses_type:
        sender_ref = {}

        @push_queue(responses_type)
        def from_web(sender) -> None:
            sender_ref["sender"] = sender

        async def make_websocket_request(state, request_id, request_value, sender):
            try:
                socket = await websocket_connect(
                    httpclient.HTTPRequest(
                        request_value.url,
                        headers=request_value.headers,
                    ),
                    ping_interval=1,
                    ping_timeout=3,
                )
            except Exception:
                if state.running:
                    sender({request_id: {"connect_response": False}})
                state.tasks.pop(request_id, None)
                return

            state.sockets[request_id] = socket
            if state.running:
                sender({request_id: {"connect_response": True}})
            while state.queues[request_id]:
                message = state.queues[request_id].popleft()
                await socket.write_message(message, binary=binary)

            try:
                while state.running:
                    message = await socket.read_message()
                    if message is None:
                        break
                    if binary and isinstance(message, str):
                        message = message.encode()
                    elif not binary and isinstance(message, bytes):
                        message = message.decode()
                    sender({request_id: {"message": message}})
            except asyncio.CancelledError:
                pass
            finally:
                socket.close()
                state.sockets.pop(request_id, None)
                state.tasks.pop(request_id, None)
                state.queues.pop(request_id, None)
                if state.running:
                    sender({request_id: {"connect_response": False}})

        def start_request(state, request_id, request_value, sender):
            previous = state.tasks.pop(request_id, None)
            if previous is not None:
                previous.cancel()
            state.tasks[request_id] = asyncio.create_task(
                make_websocket_request(state, request_id, request_value, sender)
            )

        async def send_message(state, request_id, message):
            socket = state.sockets.get(request_id)
            if socket is None:
                state.queues[request_id].append(message)
            else:
                await socket.write_message(message, binary=binary)

        def remove_request(state, request_id):
            task = state.tasks.pop(request_id, None)
            if task is not None:
                task.cancel()
            socket = state.sockets.pop(request_id, None)
            if socket is not None:
                socket.close()
            state.queues.pop(request_id, None)

        @sink_node
        def to_web(
            requests: requests_type,
            _state: STATE = None,
        ) -> None:
            sender = sender_ref["sender"]
            loop = TornadoWeb.get_loop()
            for request_id in requests.removed_keys():
                loop.add_callback(remove_request, _state, request_id)
            for request_id, request_value in requests.modified_items():
                if request_value.connect_request.modified:
                    loop.add_callback(
                        start_request,
                        _state,
                        request_id,
                        request_value.connect_request.value,
                        sender,
                    )
                if request_value.message.modified:
                    loop.add_callback(
                        send_message,
                        _state,
                        request_id,
                        request_value.message.value,
                    )

        @to_web.start
        def to_web_start(_state: STATE = None) -> None:
            TornadoWeb.start_loop()
            _state.running = True
            _state.queues = defaultdict(deque)
            _state.sockets = {}
            _state.tasks = {}

        @to_web.stop
        def to_web_stop(
            _state: STATE = None,
        ) -> None:
            completed = threading.Event()

            async def cleanup():
                try:
                    _state.running = False
                    for socket in tuple(_state.sockets.values()):
                        socket.close()
                    tasks = tuple(_state.tasks.values())
                    for task in tasks:
                        task.cancel()
                    if tasks:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    _state.sockets.clear()
                    _state.tasks.clear()
                    _state.queues.clear()
                finally:
                    completed.set()

            try:
                TornadoWeb.get_loop().add_callback(cleanup)
                if not completed.wait(timeout=5.0):
                    raise RuntimeError("WebSocket client tasks did not stop")
            finally:
                TornadoWeb.stop_loop()
                sender_ref.clear()

        to_web(request)
        return from_web()

    _CLIENT_IMPLEMENTATIONS[message_type] = websocket_client_adaptor_impl
    return websocket_client_adaptor_impl


websocket_client_adaptor_impl = _AdaptorImplGroup(
    _client_implementation(str),
    _client_implementation(bytes),
)


__all__ = (
    "websocket_client_adaptor",
    "websocket_client_adaptor_impl",
)
