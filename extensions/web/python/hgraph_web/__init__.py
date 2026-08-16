"""C++-first HTTP/S and WebSocket transports for hgraph."""

from __future__ import annotations

from . import _hgraph_web as _native  # noqa: F401 - loads the native module


# The authoring surface — schemas, service interfaces, decorators, and codec
# helpers — lands with the service surface described in RFC 0024.
__all__: list[str] = []
