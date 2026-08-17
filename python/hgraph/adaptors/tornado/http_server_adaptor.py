"""The released HTTP server surface, served by the hgraph-web transport.

The implementation moved to :mod:`hgraph_web.compat`, which reproduces this
module's decorators, request/response values, and registration functions on the
native web transport (RFC 0024, "Compatibility and migration") — including the
released defective behaviours the handlers here were written against.  The
names are re-exported so released import paths keep working unchanged; the
Tornado-based server implementation they used to name is gone.

``_bundle_field_types`` and ``_handler_parameters`` are re-exported for
:mod:`hgraph.adaptors.tornado._rest_handler`, which still owns the REST tier and
lowers it onto ``http_server_handler``.
"""

try:
    from hgraph_web.compat import (
        HttpDeleteRequest,
        HttpGetRequest,
        HttpPostRequest,
        HttpPutRequest,
        HttpRequest,
        HttpResponse,
        _bundle_field_types,
        _handler_parameters,
        http_server_adaptor,
        http_server_adaptor_impl,
        http_server_handler,
        register_http_server_adaptor,
    )
except ModuleNotFoundError as error:
    if error.name != "hgraph_web":
        raise
    raise ModuleNotFoundError(
        "hgraph.adaptors.tornado serves HTTP through the optional 'hgraph-web' "
        "distribution; install it with `pip install hgraph-web`",
        name="hgraph_web",
    ) from error


__all__ = (
    "HttpDeleteRequest",
    "HttpGetRequest",
    "HttpPostRequest",
    "HttpPutRequest",
    "HttpRequest",
    "HttpResponse",
    "http_server_handler",
    "http_server_adaptor",
    "http_server_adaptor_impl",
    "register_http_server_adaptor",
)
