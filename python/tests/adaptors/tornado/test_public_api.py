import inspect
from importlib import import_module

import hgraph.adaptors.tornado as tornado


CORE_API = {
    "Credentials",
    "HttpRequest",
    "HttpGetRequest",
    "HttpDeleteRequest",
    "HttpPutRequest",
    "HttpPostRequest",
    "HttpResponse",
    "http_server_handler",
    "register_http_server_adaptor",
    "http_client_adaptor",
    "http_client_adaptor_impl",
    "RestRequest",
    "RestCreateRequest",
    "RestUpdateRequest",
    "RestReadRequest",
    "RestDeleteRequest",
    "RestListRequest",
    "RestResponse",
    "RestCreateResponse",
    "RestUpdateResponse",
    "RestReadResponse",
    "RestDeleteResponse",
    "RestListResponse",
    "RestResultEnum",
    "rest_handler",
    "register_rest_client",
    "rest_list",
    "rest_read",
    "rest_create",
    "rest_update",
    "rest_delete",
    "WebSocketConnectRequest",
    "WebSocketServerRequest",
    "WebSocketClientRequest",
    "WebSocketResponse",
    "websocket_server_handler",
    "register_websocket_server_adaptor",
    "websocket_client_adaptor",
    "websocket_client_adaptor_impl",
}

ADVANCED_API = {
    "http_server_adaptor",
    "http_server_adaptor_impl",
    "websocket_server_adaptor",
    "websocket_server_adaptor_helper",
    "websocket_server_adaptor_impl",
}

INTERNAL_NAMES = {
    "HttpAdaptorManager",
    "HttpHandler",
    "WebSocketAdaptorManager",
    "WebSocketHandler",
    "TornadoWeb",
    "BaseHandler",
    "REST_RESPONSE",
    "STR_OR_BYTES",
}


def test_tornado_package_exports_only_the_supported_user_api():
    assert set(tornado.__all__) == CORE_API | ADVANCED_API
    assert not INTERNAL_NAMES.intersection(tornado.__all__)
    for name in CORE_API | ADVANCED_API:
        assert getattr(tornado, name) is not None


def test_public_adaptor_stubs_preserve_their_declared_signatures():
    expected = {
        "http_client_adaptor": ("request", "path"),
        "http_server_adaptor": ("response", "path"),
        "websocket_client_adaptor": ("request", "path"),
        "websocket_server_adaptor": ("response", "path"),
    }

    for name, parameters in expected.items():
        signature = inspect.signature(getattr(tornado, name))
        assert tuple(signature.parameters) == parameters
        assert signature.return_annotation is not inspect.Signature.empty

    assert inspect.signature(tornado.http_server_adaptor).parameters[
        "path"
    ].default is inspect.Signature.empty
    assert inspect.signature(tornado.websocket_server_adaptor).parameters[
        "path"
    ].default is inspect.Signature.empty


def test_supported_names_remain_available_from_their_documented_modules():
    http_client_module = import_module("hgraph.adaptors.tornado.http_client_adaptor")
    http_server_module = import_module("hgraph.adaptors.tornado.http_server_adaptor")
    rest_client_module = import_module("hgraph.adaptors.tornado._rest_client")
    rest_handler_module = import_module("hgraph.adaptors.tornado._rest_handler")
    websocket_client_module = import_module("hgraph.adaptors.tornado.websocket_client_adaptor")
    websocket_server_module = import_module("hgraph.adaptors.tornado.websocket_server_adaptor")

    module_names = {
        http_client_module: {
            "Credentials", "http_client_adaptor", "http_client_adaptor_impl",
        },
        http_server_module: {
            "HttpRequest", "HttpGetRequest", "HttpDeleteRequest",
            "HttpPutRequest", "HttpPostRequest", "HttpResponse",
            "http_server_handler", "register_http_server_adaptor",
            "http_server_adaptor", "http_server_adaptor_impl",
        },
        rest_handler_module: {
            "RestRequest", "RestCreateRequest", "RestUpdateRequest",
            "RestReadRequest", "RestDeleteRequest", "RestListRequest",
            "RestResponse", "RestCreateResponse", "RestUpdateResponse",
            "RestReadResponse", "RestDeleteResponse", "RestListResponse",
            "RestResultEnum", "rest_handler",
        },
        rest_client_module: {
            "register_rest_client", "rest_list", "rest_read", "rest_create",
            "rest_update", "rest_delete",
        },
        websocket_server_module: {
            "WebSocketConnectRequest", "WebSocketServerRequest",
            "WebSocketClientRequest", "WebSocketResponse",
            "websocket_server_handler", "register_websocket_server_adaptor",
            "websocket_server_adaptor", "websocket_server_adaptor_helper",
            "websocket_server_adaptor_impl",
        },
        websocket_client_module: {
            "websocket_client_adaptor", "websocket_client_adaptor_impl",
        },
    }

    for module, names in module_names.items():
        for name in names:
            assert getattr(module, name) is not None


def test_handler_signatures_exclude_all_runtime_injectables():
    # Review catch (PR #242): typed STATE[T] annotations (_StateExpr) are
    # runtime-supplied like the marker injectables — a handler declaring one
    # must not expose it as a public parameter (nor receive it from a
    # caller). The filter is the canonical _is_hidden_node_parameter.
    import inspect

    from hgraph import STATE, TS, TSB, compute_node
    from hgraph.adaptors.tornado.websocket_server_adaptor import (
        WebSocketServerRequest,
        WebSocketResponse,
        websocket_server_handler,
    )
    from hgraph.adaptors.tornado.http_server_adaptor import _handler_parameters

    class _S:
        count: int = 0

    @websocket_server_handler(url="/state-probe")
    @compute_node
    def handler(request: TSB[WebSocketServerRequest[str]],
                _state: STATE[_S] = None) -> TSB[WebSocketResponse[str]]:
        """typed-state handler"""

    assert "request" not in inspect.signature(handler).parameters
    assert "_state" not in inspect.signature(handler).parameters

    # And the shared http-side filter agrees for every injectable family.
    from hgraph import CLOCK, LOGGER, SCHEDULER

    def probe(request: TS[str], _state: STATE[_S] = None, _clock: CLOCK = None,
              _log: LOGGER = None, _sched: SCHEDULER = None,
              plain: TS[int] = None) -> TS[str]:
        """probe"""

    names = [p.name for p in _handler_parameters(inspect.signature(probe))]
    assert names == ["plain"]
