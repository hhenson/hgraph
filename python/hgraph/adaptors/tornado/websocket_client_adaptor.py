"""The released WebSocket client surface, wired to native libcurl by hgraph-web."""

try:
    from hgraph_web.compat_client import (
        websocket_client_adaptor,
        websocket_client_adaptor_impl,
    )
except ModuleNotFoundError as error:
    if error.name != "hgraph_web":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.tornado connects WebSockets through the optional "
        "'hgraph-web' distribution; install it with `pip install hgraph-web`",
        name="hgraph_web",
    ) from error

__all__ = ("websocket_client_adaptor", "websocket_client_adaptor_impl")
