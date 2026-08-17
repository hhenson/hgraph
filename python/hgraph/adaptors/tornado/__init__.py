"""The released HTTP, REST, and WebSocket adaptor surface.

The server transport is the native one: ``http_server_handler``,
``websocket_server_handler``, their adaptors, and the released request/response
values are re-exported from :mod:`hgraph_web.compat`, which reproduces the
released semantics — defects included — on hgraph-web (RFC 0024).  The
``hgraph[web]`` extra installs both Tornado and hgraph-web.

Every name is exported lazily (PEP 562): consumers of the Tornado plumbing
alone — :mod:`hgraph.adaptors.perspective` and the inspector import
``._tornado_web`` directly — must keep working without hgraph-web installed,
and the pointed ``ModuleNotFoundError`` for a missing hgraph-web should fire
when a server name is USED, not when this package is imported.

The client adaptors and the REST tier are still Tornado-backed.  New code
should use :mod:`hgraph_web` directly.
"""

_EXPORTS = {
    "http_client_adaptor": (
        "Credentials",
        "http_client_adaptor",
        "http_client_adaptor_impl",
    ),
    "http_server_adaptor": (
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
    ),
    "websocket_server_adaptor": (
        "WebSocketClientRequest",
        "WebSocketConnectRequest",
        "WebSocketResponse",
        "WebSocketServerRequest",
        "register_websocket_server_adaptor",
        "websocket_server_adaptor",
        "websocket_server_adaptor_helper",
        "websocket_server_adaptor_impl",
        "websocket_server_handler",
    ),
    "websocket_client_adaptor": (
        "websocket_client_adaptor",
        "websocket_client_adaptor_impl",
    ),
    "_rest_handler": (
        "RestCreateRequest",
        "RestCreateResponse",
        "RestDeleteRequest",
        "RestDeleteResponse",
        "RestListRequest",
        "RestListResponse",
        "RestReadRequest",
        "RestReadResponse",
        "RestRequest",
        "RestResponse",
        "RestResultEnum",
        "RestUpdateRequest",
        "RestUpdateResponse",
        "rest_handler",
    ),
    "_rest_client": (
        "register_rest_client",
        "rest_create",
        "rest_delete",
        "rest_list",
        "rest_read",
        "rest_update",
    ),
}

_NAME_TO_MODULE = {
    name: module for module, names in _EXPORTS.items() for name in names
}

__all__ = tuple(sorted(_NAME_TO_MODULE))

# Four exported names collide with their submodule's name
# (``http_server_adaptor`` is both).  When such a submodule finishes
# importing, the import system binds the MODULE over the package attribute,
# which would shadow the exported symbol for every later access (the eager
# ``from``-import used to win by running last).  Redirect that one binding
# back to the symbol.
import sys as _sys
import types as _types


class _CollisionAwareModule(_types.ModuleType):
    def __setattr__(self, name, value):
        if (
            name in _NAME_TO_MODULE
            and name == _NAME_TO_MODULE[name]
            and isinstance(value, _types.ModuleType)
            and hasattr(value, name)
        ):
            value = getattr(value, name)
        super().__setattr__(name, value)


_sys.modules[__name__].__class__ = _CollisionAwareModule


def __getattr__(name: str):
    module_name = _NAME_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    module = import_module(f".{module_name}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
