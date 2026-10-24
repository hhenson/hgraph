"""Released client adaptors wired to the native libcurl services.

Only legacy values and graph shapes are adapted here. Connections, queues,
authentication, timeouts, cancellation and thread lifetime belong to libcurl's
native graph-scoped client implementation.
"""

from math import ceil
from urllib.parse import urlencode, urlsplit, urlunsplit

from hgraph import (
    STATE, TS, TSB, TSD, TimeSeriesSchema, combine, compute_node, gate, graph, map_, merge, sample,
    service_adaptor, service_adaptor_impl,
)
from hgraph._wiring._services import _AdaptorImplGroup

from . import (
    HttpCallResult, HttpClientOptions, HttpClientRequest,
    HttpMethod, WebClientConfig, WebHeader, WsClientKey, WsClientOutput,
    WsConnectionState, WsFrame, WsFrameKind, register_web_client,
    web_http_request, web_ws_client_send, web_ws_connect,
)
from .compat import (
    STR_OR_BYTES, HttpDeleteRequest, HttpGetRequest, HttpPostRequest,
    HttpPutRequest, HttpRequest, HttpResponse, WebSocketClientRequest,
    WebSocketConnectRequest, WebSocketResponse, _collapse_headers,
)

__all__ = (
    "Credentials", "http_client_adaptor", "http_client_adaptor_impl",
    "websocket_client_adaptor", "websocket_client_adaptor_impl",
)


class Credentials:
    def __init__(self, username, password):
        self.username = username
        self.password = password

    def __repr__(self) -> str:
        return "credentials"

    __str__ = __repr__


@service_adaptor
def http_client_adaptor(
    request: TS[HttpRequest], path: str = "http_client"
) -> TS[HttpResponse]:
    """Send a GET, POST, PUT, or DELETE request through the native client."""


class _HttpCall(TimeSeriesSchema):
    request: TS[HttpClientRequest]
    options: TS[HttpClientOptions]
    error: TS[HttpResponse]


@compute_node
def _http_call(request: TS[HttpRequest]) -> TSB[_HttpCall]:
    value = request.value
    try:
        for request_type, method in (
            (HttpGetRequest, HttpMethod.GET),
            (HttpPostRequest, HttpMethod.POST),
            (HttpPutRequest, HttpMethod.PUT),
            (HttpDeleteRequest, HttpMethod.DELETE),
        ):
            if isinstance(value, request_type):
                break
        else:
            raise ValueError(f"unsupported HTTP request: {type(value).__name__}")
        url = urlsplit(value.url)
        query = urlencode(value.query)
        if query:
            url = url._replace(query=f"{url.query}&{query}" if url.query else query)
        body = getattr(value, "body", "")
        credentials = value.auth if isinstance(value.auth, Credentials) else None
        return {
            "request": HttpClientRequest(
                method=method, url=urlunsplit(url),
                headers=tuple(WebHeader(k, v) for k, v in value.headers.items()),
                body=body.encode() if isinstance(body, str) else body,
            ),
            "options": HttpClientOptions(
                connect_timeout_ms=ceil(value.connect_timeout * 1000),
                request_timeout_ms=ceil(value.request_timeout * 1000),
                integrated_auth=credentials is not None,
                username=credentials.username if credentials else "",
                password=credentials.password if credentials else "",
            ),
        }
    except Exception as error:
        # The released client reports malformed requests as responses too.
        return {"error": HttpResponse(status_code=400, body=str(error).encode())}


@compute_node(valid=())
def _http_response(result: TSB[HttpCallResult]) -> TS[HttpResponse]:
    if result.response.modified and result.response.valid:
        value = result.response.value
        return HttpResponse(
            status_code=value.status, headers=_collapse_headers(value.headers),
            body=value.body,
        )
    if result.failure.modified and result.failure.valid:
        return HttpResponse(status_code=400, body=result.failure.value.message.encode())


@service_adaptor_impl(interfaces=http_client_adaptor)
def http_client_adaptor_impl(
    request: TSD[int, TS[HttpRequest]], path: str = "http_client",
    use_curl: bool = False, max_clients: int = 50,
) -> TSD[int, TS[HttpResponse]]:
    """Multiplex legacy callers over one native libcurl client.

    ``use_curl`` is retained for call compatibility; both values use libcurl.
    ``max_clients`` limits both total and per-host native connections.
    """
    service_path = f"web-compat-http-client-{path}"
    register_web_client(WebClientConfig(
        max_total_connections=max_clients, max_connections_per_host=max_clients,
        # Native calls reserve their largest response before admission. Keep
        # enough reservations for the released client's concurrency setting.
        ingress_byte_limit=max(64 * 1024 * 1024, max_clients * 16 * 1024 * 1024),
        shutdown_drain_timeout_ms=250,
    ), path=service_path)

    @graph
    def call(request: TS[HttpRequest]) -> TS[HttpResponse]:
        converted = _http_call(request)
        result = web_http_request(converted.request, converted.options, path=service_path)
        return merge(_http_response(result), converted.error)

    return map_(call, request)


@service_adaptor
def websocket_client_adaptor(
    request: TSB[WebSocketClientRequest[STR_OR_BYTES]],
    path: str = "websocket_client",
) -> TSB[WebSocketResponse[STR_OR_BYTES]]:
    """Open a typed native WebSocket connection and exchange messages."""


class _WsConnect(TimeSeriesSchema):
    key: TS[WsClientKey]
    opening: TS[bool]
    error: TS[bool]


@compute_node(active=("request",))
def _ws_client_key(
    request: TS[WebSocketConnectRequest], key: TS[int], _state: STATE = None,
) -> TSB[_WsConnect]:
    _state.generation = getattr(_state, "generation", 0) + 1
    value = request.value
    try:
        return {"key": WsClientKey(
            url=value.url,
            headers=tuple(WebHeader(k, v) for k, v in value.headers.items()),
            identity=f"{key.value}:{_state.generation}",
        ), "opening": False}
    except ValueError:
        return {"error": False}


@compute_node
def _client_text_frame(message: TS[str]) -> TS[WsFrame]:
    return WsFrame.text_frame(message.value)


@compute_node
def _client_binary_frame(message: TS[bytes]) -> TS[WsFrame]:
    return WsFrame.binary_frame(message.value)


@compute_node(valid=())
def _client_text_response(result: TSB[WsClientOutput]) -> TSB[WebSocketResponse[str]]:
    response = {}
    if result.event.modified and result.event.valid:
        response["connect_response"] = result.event.value.state == WsConnectionState.OPEN
    if result.frame.modified and result.frame.valid:
        frame = result.frame.value
        if frame.kind == WsFrameKind.TEXT:
            response["message"] = frame.text
        elif frame.kind == WsFrameKind.BINARY:
            response["message"] = frame.data.decode()
    return response or None


@compute_node(valid=())
def _client_binary_response(result: TSB[WsClientOutput]) -> TSB[WebSocketResponse[bytes]]:
    response = {}
    if result.event.modified and result.event.valid:
        response["connect_response"] = result.event.value.state == WsConnectionState.OPEN
    if result.frame.modified and result.frame.valid:
        frame = result.frame.value
        if frame.kind == WsFrameKind.TEXT:
            response["message"] = frame.text.encode()
        elif frame.kind == WsFrameKind.BINARY:
            response["message"] = frame.data
    return response or None


def _client_implementation(message_type: type):
    interface = websocket_client_adaptor[STR_OR_BYTES:message_type]
    request_bundle = TSB[WebSocketClientRequest[message_type]]
    response_bundle = TSB[WebSocketResponse[message_type]]
    requests_type = TSD[int, request_bundle]
    responses_type = TSD[int, response_bundle]
    to_frame = _client_text_frame if message_type is str else _client_binary_frame
    from_native = _client_text_response if message_type is str else _client_binary_response

    @service_adaptor_impl(interfaces=interface)
    def websocket_client_adaptor_impl(
        request: requests_type, path: str = "websocket_client",
    ) -> responses_type:
        service_path = f"web-compat-ws-client-{message_type.__name__}-{path}"
        register_web_client(WebClientConfig(
            ping_interval_ms=1000, pong_timeout_ms=3000,
            shutdown_drain_timeout_ms=250,
        ), path=service_path)

        @graph
        def connect(key: TS[int], request: request_bundle) -> response_bundle:
            connection = _ws_client_key(request.connect_request, key)
            result = web_ws_connect(connection.key, path=service_path)
            response = from_native(result)
            connected = merge(response.connect_response, connection.error)
            # Retain early messages until the native handshake has completed.
            ready = merge(response.connect_response, connection.opening, connection.error)
            frames = gate(ready, to_frame(request.message))
            # Sample only on a fresh frame: a reconnect must not resend it.
            send_key = sample(frames, connection.key)
            web_ws_client_send(send_key, frames, path=service_path)
            return combine[response_bundle](
                connect_response=connected,
                message=response.message,
            )

        return map_(connect, request)

    return websocket_client_adaptor_impl


websocket_client_adaptor_impl = _AdaptorImplGroup(
    _client_implementation(str), _client_implementation(bytes),
)
