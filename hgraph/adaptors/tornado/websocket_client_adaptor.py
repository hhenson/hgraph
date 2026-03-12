import asyncio
from collections import defaultdict, deque
from datetime import timedelta
from typing import Callable, Type

from tornado import httpclient
from tornado.websocket import websocket_connect

from hgraph import (
    AUTO_RESOLVE,
    MIN_DT,
    GlobalState,
    SCHEDULER,
    STATE,
    TSB,
    TSD,
    push_queue,
    service_adaptor,
    service_adaptor_impl,
    sink_node,
)
from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.websocket_server_adaptor import STR_OR_BYTES, WebSocketClientRequest, WebSocketResponse


@service_adaptor
def websocket_client_adaptor(
    request: TSB[WebSocketClientRequest[STR_OR_BYTES]], path: str = "websocket_client"
) -> TSB[WebSocketResponse[STR_OR_BYTES]]: ...


@service_adaptor_impl(interfaces=websocket_client_adaptor)
def websocket_client_adaptor_impl(
    request: TSD[int, TSB[WebSocketClientRequest[STR_OR_BYTES]]],
    path: str = "websocket_client",
    _tp: Type[STR_OR_BYTES] = AUTO_RESOLVE,
) -> TSD[int, TSB[WebSocketResponse[STR_OR_BYTES]]]:

    is_binary = _tp is bytes

    @push_queue(TSD[int, TSB[WebSocketResponse[_tp]]])
    def from_web(sender, path: str = "websocket_client") -> TSD[int, TSB[WebSocketResponse[_tp]]]:
        GlobalState.instance()[f"websocket_client_adaptor://{path}/queue"] = sender
        return None

    async def make_websocket_request(state: STATE, id: int, request: WebSocketClientRequest, sender: Callable):
        try:
            ws = await websocket_connect(
                httpclient.HTTPRequest(request.url, headers=request.headers), ping_interval=1, ping_timeout=3
            )
        except Exception as e:
            sender({id: {"connect_response": False}})
            return

        state.sockets[id] = ws
        sender({id: {"connect_response": True}})

        try:
            while True:
                msg = await ws.read_message()
                if msg is None:
                    break
                if type(msg) is str and is_binary:
                    msg = msg.encode("utf-8")
                elif type(msg) is bytes and not is_binary:
                    msg = msg.decode("utf-8")
                sender({id: {"message": msg}})
                # Avoid coalescing multiple websocket frames for the same key
                # into a single graph update in one engine cycle.
                await asyncio.sleep(0)
        finally:
            ws.close()
            del state.sockets[id]
            sender({id: {"connect_response": False}})

    async def send_websocket_message(state: STATE, id: int, message: STR_OR_BYTES):
        if ws := state.sockets.get(id):
            await ws.write_message(message, binary=is_binary)
        else:
            state.queues[id].append(message)

    @sink_node
    def to_web(
        request: TSD[int, TSB[WebSocketClientRequest[STR_OR_BYTES]]],
        _state: STATE = None,
        _scheduler: SCHEDULER = None,
    ):
        sender = GlobalState.instance()[f"websocket_client_adaptor://{path}/queue"]

        for i, r in request.modified_items():
            if r.connect_request.modified:
                TornadoWeb.get_loop().add_callback(make_websocket_request, _state, i, r.connect_request.value, sender)
            if r.message.modified:
                _state.last_sent[i] = r.message.last_modified_time
                TornadoWeb.get_loop().add_callback(send_websocket_message, _state, i, r.message.value)

        active_requests = False
        for i, r in request.items():
            active_requests = True
            if not r.message.valid:
                continue
            last_sent = _state.last_sent.get(i, MIN_DT)
            if r.message.last_modified_time > last_sent:
                _state.last_sent[i] = r.message.last_modified_time
                TornadoWeb.get_loop().add_callback(send_websocket_message, _state, i, r.message.value)

        for i in request.removed_keys():
            _state.last_sent.pop(i, None)

        if active_requests and _scheduler is not None:
            _scheduler.schedule(timedelta(milliseconds=1), "websocket_client_poll", on_wall_clock=True)

    @to_web.start
    def to_web_start(_state: STATE):
        TornadoWeb.start_loop()
        _state.queues = defaultdict(deque)
        _state.sockets = {}
        _state.last_sent = {}

    @to_web.stop
    def to_web_stop():
        TornadoWeb.stop_loop()

    to_web(request)
    return from_web()
