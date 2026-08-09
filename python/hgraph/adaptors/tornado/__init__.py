"""Tornado-backed HTTP adaptors.

The transport is optional; install ``hgraph[web]`` before importing this
package.
"""

from .http_client_adaptor import (
    Credentials,
    http_client_adaptor,
    http_client_adaptor_impl,
)
from .http_server_adaptor import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
    http_server_handler,
    http_server_adaptor,
    http_server_adaptor_impl,
    register_http_server_adaptor,
)
from .websocket_server_adaptor import (
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
from .websocket_client_adaptor import (
    websocket_client_adaptor,
    websocket_client_adaptor_impl,
)
from ._rest_handler import (
    RestCreateRequest,
    RestCreateResponse,
    RestDeleteRequest,
    RestDeleteResponse,
    RestListRequest,
    RestListResponse,
    RestReadRequest,
    RestReadResponse,
    RestRequest,
    RestResponse,
    RestResultEnum,
    RestUpdateRequest,
    RestUpdateResponse,
    rest_handler,
)
from ._rest_client import (
    register_rest_client,
    rest_create,
    rest_delete,
    rest_list,
    rest_read,
    rest_update,
)

__all__ = (
    "Credentials",
    "HttpDeleteRequest",
    "HttpGetRequest",
    "HttpPostRequest",
    "HttpPutRequest",
    "HttpRequest",
    "HttpResponse",
    "http_client_adaptor",
    "http_client_adaptor_impl",
    "http_server_adaptor",
    "http_server_adaptor_impl",
    "http_server_handler",
    "register_http_server_adaptor",
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
    "register_rest_client",
    "rest_create",
    "rest_delete",
    "rest_list",
    "rest_read",
    "rest_update",
    "WebSocketClientRequest",
    "WebSocketConnectRequest",
    "WebSocketResponse",
    "WebSocketServerRequest",
    "register_websocket_server_adaptor",
    "websocket_server_adaptor",
    "websocket_server_adaptor_helper",
    "websocket_server_adaptor_impl",
    "websocket_server_handler",
    "websocket_client_adaptor",
    "websocket_client_adaptor_impl",
)
