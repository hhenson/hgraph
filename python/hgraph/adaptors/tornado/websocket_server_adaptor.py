"""The released WebSocket server surface, served by the hgraph-web transport.

The implementation moved to :mod:`hgraph_web.compat`, which reproduces this
module's handler decorator, typed request/response schemas, adaptor, and
registration tokens on the native web transport (RFC 0024, "Compatibility and
migration").  The names are re-exported so released import paths keep working
unchanged; the Tornado-based server implementation they used to name is gone.

``STR_OR_BYTES`` is re-exported for
:mod:`hgraph.adaptors.tornado.websocket_client_adaptor`, which still owns the
client tier and shares the message-type variable.
"""

try:
    from hgraph_web.compat import (
        STR_OR_BYTES,
        WebSocketClientRequest,
        WebSocketConnectRequest,
        WebSocketResponse,
        WebSocketServerRequest,
        register_websocket_server_adaptor,
        websocket_server_adaptor,
        websocket_server_adaptor_helper,
        websocket_server_adaptor_impl,
        websocket_server_handler,
    )
except ModuleNotFoundError as error:
    if error.name != "hgraph_web":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.tornado serves WebSockets through the optional "
        "'hgraph-web' distribution; install it with `pip install hgraph-web`",
        name="hgraph_web",
    ) from error


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
