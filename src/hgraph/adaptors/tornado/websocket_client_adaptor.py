from collections import defaultdict, deque
from typing import Callable

from tornado import httpclient
from tornado.websocket import websocket_connect

from hgraph import service_adaptor, TS, service_adaptor_impl, TSD, push_queue, GlobalState, sink_node, STATE, TSB
from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.websocket_server_adaptor import WebSocketClientRequest, WebSocketClientRequest, WebSocketResponse


@service_adaptor
def websocket_client_adaptor(request: TSB[WebSocketClientRequest], path: str = "websocket_client") -> TSB[WebSocketResponse]: ...


@service_adaptor_impl(interfaces=websocket_client_adaptor)
def websocket_client_adaptor_impl(
    request: TSD[int, TSB[WebSocketClientRequest]], path: str = "websocket_client"
) -> TSD[int, TSB[WebSocketResponse]]:

    @push_queue(TSD[int, TSB[WebSocketResponse]])
    def from_web(sender, path: str = "websocket_client") -> TSD[int, TSB[WebSocketResponse]]:
        GlobalState.instance()[f"websocket_client_adaptor://{path}/queue"] = sender
        return None

    async def make_websocket_request(state: STATE, id: int, request: WebSocketClientRequest, sender: Callable):
        try:
            ws = await websocket_connect(httpclient.HTTPRequest(request.url, headers=request.headers),
                                         ping_interval=1,
                                         ping_timeout=3)
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
                sender({id: {"message": msg}})
        finally:
            ws.close()
            del state.sockets[id]
            sender({id: {"connect_response": False}})

    async def send_websocket_message(state: STATE, id: int, message: bytes):
        if ws := state.sockets.get(id):
            await ws.write_message(message, binary=True)
        else:
            state.queues[id].append(message)

    @sink_node
    def to_web(request: TSD[int, TSB[WebSocketClientRequest]], _state: STATE = None):
        sender = GlobalState.instance()[f"websocket_client_adaptor://{path}/queue"]

        for i, r in request.modified_items():
            if r.connect_request.modified:
                TornadoWeb.get_loop().add_callback(make_websocket_request, _state, i, r.connect_request.value, sender)
            if r.message.modified:
                TornadoWeb.get_loop().add_callback(send_websocket_message, _state, i, r.message.value)

    @to_web.start
    def to_web_start(_state: STATE):
        TornadoWeb.start_loop()
        _state.queues = defaultdict(deque)
        _state.sockets = {}

    @to_web.stop
    def to_web_stop():
        TornadoWeb.stop_loop()

    to_web(request)
    return from_web()
