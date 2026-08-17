"""C++-first HTTP/S and WebSocket transports for hgraph.

The public values, time-series collections, and service interfaces here mirror
the C++ schemas in ``hgraph/web/types.h`` and the service descriptors in
``hgraph/web/service.h`` one-for-one: field names, field order, and descriptor
names are the wire contract shared with the native transport (RFC 0024).
Wiring-time validation mirrors ``value_builders.cpp`` so a misconfigured graph
fails while it is being wired rather than when a socket is bound.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from hgraph import (
    CompoundScalar,
    TS,
    TSB,
    TimeSeriesSchema,
    const,
    operator_function,
    reference_service,
    request_reply_service,
    subscription_service,
)

from ._hgraph_web import (
    HttpMethod,
    WebClientVerify,
    WebDeliveryStatus,
    WebFailurePolicy,
    WebHttpVersionPolicy,
    WebInboundOverflow,
    WebOverflowAction,
    WebRouteState,
    WebSeverity,
    WebSlowConsumerPolicy,
    WebTlsVersion,
    WsConnectionState,
    WsFrameKind,
)


_NAMESPACE = "hgraph.web"


def _require_positive(value: int, what: str) -> None:
    if value <= 0:
        raise ValueError(f"Web {what} must be positive")


def _require_non_negative(value: int, what: str) -> None:
    if value < 0:
        raise ValueError(f"Web {what} cannot be negative")


def _require_pct_watermarks(high: int, low: int) -> None:
    if low <= 0 or high >= 100 or low >= high:
        raise ValueError("Web watermarks require 0 < low < high < 100")


def _require_path_xor_pem(path: str, pem: str, what: str) -> None:
    if path and pem:
        raise ValueError(
            f"Web TLS {what} accepts a filesystem path or inline PEM, not both"
        )


# Headers, query parameters, and path captures are ALWAYS ordered sequences of
# name/value pairs, never maps: duplicate names and arrival order are preserved
# by construction (RFC 0024, public value contract).
@dataclass(frozen=True)
class WebHeader(CompoundScalar, namespace=_NAMESPACE):
    name: str
    value: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Web header names cannot be empty")


@dataclass(frozen=True)
class WebParam(CompoundScalar, namespace=_NAMESPACE):
    name: str
    value: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Web parameter names cannot be empty")


@dataclass(frozen=True)
class WebPeer(CompoundScalar, namespace=_NAMESPACE):
    remote_address: str = ""
    remote_port: int = 0
    local_port: int = 0
    tls: bool = False
    negotiated_protocol: str = ""
    sni: str = ""
    client_cert_subject: str = ""


@dataclass(frozen=True)
class WebRoute(CompoundScalar, namespace=_NAMESPACE):
    method: HttpMethod
    pattern: str
    upgrade: bool = False

    def __post_init__(self) -> None:
        if not self.pattern or not self.pattern.startswith("/"):
            raise ValueError("Web route patterns must start with '/'")


@dataclass(frozen=True)
class HttpRequest(CompoundScalar, namespace=_NAMESPACE):
    method: HttpMethod
    target: str = ""
    path: str = ""
    query: tuple[WebParam, ...] = ()
    path_params: tuple[WebParam, ...] = ()
    headers: tuple[WebHeader, ...] = ()
    body: bytes = b""
    trailers: tuple[WebHeader, ...] = ()


@dataclass(frozen=True)
class HttpServerRequest(CompoundScalar, namespace=_NAMESPACE):
    request_id: int = 0
    connection_id: int = 0
    stream_id: int = 0
    request: Optional[HttpRequest] = None
    peer: Optional[WebPeer] = None


@dataclass(frozen=True)
class HttpResponse(CompoundScalar, namespace=_NAMESPACE):
    status: int
    headers: tuple[WebHeader, ...] = ()
    body: bytes = b""
    trailers: tuple[WebHeader, ...] = ()

    def __post_init__(self) -> None:
        if self.status < 100 or self.status > 599:
            raise ValueError("Web response status must be 100..599")


@dataclass(frozen=True)
class WebTransportError(CompoundScalar, namespace=_NAMESPACE):
    error_code: int = 0
    message: str = ""
    retriable: bool = False


@dataclass(frozen=True)
class HttpClientRequest(CompoundScalar, namespace=_NAMESPACE):
    method: HttpMethod
    url: str
    headers: tuple[WebHeader, ...] = ()
    body: bytes = b""

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("Web client requests require a URL")


@dataclass(frozen=True)
class HttpClientOptions(CompoundScalar, namespace=_NAMESPACE):
    connect_timeout_ms: int = 10_000
    request_timeout_ms: int = 30_000
    follow_redirects: bool = True
    max_redirects: int = 5
    http_version: WebHttpVersionPolicy = WebHttpVersionPolicy.AUTO

    def __post_init__(self) -> None:
        _require_non_negative(self.connect_timeout_ms, "connect timeout")
        _require_non_negative(self.request_timeout_ms, "request timeout")
        _require_non_negative(self.max_redirects, "max_redirects")


@dataclass(frozen=True)
class WsFrame(CompoundScalar, namespace=_NAMESPACE):
    kind: WsFrameKind
    text: Optional[str] = None
    data: Optional[bytes] = None
    close_code: Optional[int] = None
    close_reason: Optional[str] = None

    def __post_init__(self) -> None:
        if self.close_code is not None and not 1000 <= self.close_code <= 4999:
            raise ValueError("WebSocket close codes must be 1000..4999")

    @classmethod
    def text_frame(cls, text: str) -> "WsFrame":
        return cls(WsFrameKind.TEXT, text=text)

    @classmethod
    def binary_frame(cls, data: bytes) -> "WsFrame":
        return cls(WsFrameKind.BINARY, data=data)

    @classmethod
    def close_frame(cls, close_code: int = 1000, close_reason: str = "") -> "WsFrame":
        return cls(WsFrameKind.CLOSE, close_code=close_code, close_reason=close_reason)


@dataclass(frozen=True)
class WsInboundFrame(CompoundScalar, namespace=_NAMESPACE):
    connection_id: int = 0
    frame: Optional[WsFrame] = None


@dataclass(frozen=True)
class WsEvent(CompoundScalar, namespace=_NAMESPACE):
    connection_id: int = 0
    state: WsConnectionState = WsConnectionState.OPEN
    request: Optional[HttpServerRequest] = None
    close_code: Optional[int] = None
    close_reason: Optional[str] = None


@dataclass(frozen=True)
class WsClientKey(CompoundScalar, namespace=_NAMESPACE):
    url: str
    headers: tuple[WebHeader, ...] = ()
    subprotocols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.url:
            raise ValueError("WebSocket client keys require a URL")


@dataclass(frozen=True)
class WebDeliveryReport(CompoundScalar, namespace=_NAMESPACE):
    request_id: int = 0
    sequence: int = 0
    status: WebDeliveryStatus = WebDeliveryStatus.DELIVERED
    error_code: int = 0
    retriable: bool = False
    fatal: bool = False
    message: str = ""


@dataclass(frozen=True)
class WebEvent(CompoundScalar, namespace=_NAMESPACE):
    severity: WebSeverity = WebSeverity.INFO
    component: str = ""
    category: str = ""
    error_code: int = 0
    retriable: bool = False
    fatal: bool = False
    service_path: str = ""
    connection_id: int = 0
    message: str = ""


@dataclass(frozen=True)
class WebServerStats(CompoundScalar, namespace=_NAMESPACE):
    listening_port: int = 0
    connection_count: int = 0
    ws_connection_count: int = 0
    pending_request_count: int = 0
    ingress_record_count: int = 0
    ingress_byte_count: int = 0
    outbound_byte_count: int = 0
    dropped_count: int = 0


@dataclass(frozen=True)
class WebClientStats(CompoundScalar, namespace=_NAMESPACE):
    in_flight_count: int = 0
    staged_count: int = 0
    ws_connection_count: int = 0
    ingress_record_count: int = 0
    ingress_byte_count: int = 0
    dropped_count: int = 0


@dataclass(frozen=True)
class TlsServerConfig(CompoundScalar, namespace=_NAMESPACE):
    cert_path: str = ""
    cert_pem: str = ""
    key_path: str = ""
    key_pem: str = ""
    key_password: str = ""
    ca_path: str = ""
    ca_pem: str = ""
    client_verify: WebClientVerify = WebClientVerify.NONE
    alpn: tuple[str, ...] = ("http/1.1",)
    min_version: WebTlsVersion = WebTlsVersion.TLS1_2

    def __post_init__(self) -> None:
        _require_path_xor_pem(self.cert_path, self.cert_pem, "certificate")
        _require_path_xor_pem(self.key_path, self.key_pem, "key")
        _require_path_xor_pem(self.ca_path, self.ca_pem, "CA")
        if not self.cert_path and not self.cert_pem:
            raise ValueError("Web server TLS requires a certificate chain")
        if not self.key_path and not self.key_pem:
            raise ValueError("Web server TLS requires a private key")
        if self.client_verify != WebClientVerify.NONE and not self.ca_path and not self.ca_pem:
            raise ValueError("Web client-certificate verification requires a CA")
        # The HTTP/2 session is active behind the ALPN seam (RFC 0024,
        # activation plan): "h2" is a legal server protocol.  Only known
        # protocols may be advertised.
        if any(protocol not in ("h2", "http/1.1") for protocol in self.alpn):
            raise ValueError('Web server ALPN accepts only "h2" and "http/1.1"')


@dataclass(frozen=True)
class TlsClientConfig(CompoundScalar, namespace=_NAMESPACE):
    ca_path: str = ""
    ca_pem: str = ""
    cert_path: str = ""
    cert_pem: str = ""
    key_path: str = ""
    key_pem: str = ""
    sni: str = ""
    verify_peer: bool = True
    verify_host: bool = True
    alpn: tuple[str, ...] = ()
    min_version: WebTlsVersion = WebTlsVersion.TLS1_2

    def __post_init__(self) -> None:
        _require_path_xor_pem(self.ca_path, self.ca_pem, "CA")
        _require_path_xor_pem(self.cert_path, self.cert_pem, "certificate")
        _require_path_xor_pem(self.key_path, self.key_pem, "key")
        has_cert = bool(self.cert_path or self.cert_pem)
        has_key = bool(self.key_path or self.key_pem)
        if has_cert != has_key:
            raise ValueError("Web client mTLS requires both a certificate and a private key")


# The h2_* fields are deliberately present before the server HTTP/2 session
# activates so activation is not a schema break (RFC 0024).
@dataclass(frozen=True)
class WebServerConfig(CompoundScalar, namespace=_NAMESPACE):
    bind_address: str = "0.0.0.0"
    port: int = 0
    tls: Optional[TlsServerConfig] = None
    io_threads: int = 1
    max_connections: int = 10_000
    max_header_bytes: int = 64 * 1024
    max_body_bytes: int = 16 * 1024 * 1024
    request_timeout_ms: int = 30_000
    idle_timeout_ms: int = 60_000
    keep_alive_timeout_ms: int = 15_000
    bind_deferred: bool = False
    ingress_record_limit: int = 10_000
    ingress_byte_limit: int = 64 * 1024 * 1024
    ws_ingress_record_limit: int = 10_000
    ws_ingress_byte_limit: int = 64 * 1024 * 1024
    watermark_high_pct: int = 80
    watermark_low_pct: int = 50
    inbound_overflow: WebInboundOverflow = WebInboundOverflow.BACKPRESSURE
    outbound_message_limit: int = 1_000
    outbound_byte_limit: int = 16 * 1024 * 1024
    slow_consumer_policy: WebSlowConsumerPolicy = WebSlowConsumerPolicy.CLOSE
    failure_policy: WebFailurePolicy = WebFailurePolicy.REPORT
    shutdown_drain_timeout_ms: int = 5_000
    ws_max_frame_bytes: int = 1024 * 1024
    ws_max_message_bytes: int = 16 * 1024 * 1024
    ping_interval_ms: int = 30_000
    pong_timeout_ms: int = 10_000
    stats_interval_ms: int = 0
    h2_max_concurrent_streams: int = 100
    h2_initial_window_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        if not self.bind_address:
            raise ValueError("Web server bind address cannot be empty")
        if self.port < 0 or self.port > 65_535:
            raise ValueError("Web server port must be 0..65535")
        _require_positive(self.io_threads, "io_threads")
        _require_positive(self.max_connections, "max_connections")
        _require_positive(self.max_header_bytes, "max_header_bytes")
        _require_positive(self.max_body_bytes, "max_body_bytes")
        _require_non_negative(self.request_timeout_ms, "request timeout")
        _require_non_negative(self.idle_timeout_ms, "idle timeout")
        _require_non_negative(self.keep_alive_timeout_ms, "keep-alive timeout")
        _require_positive(self.ingress_record_limit, "ingress record limit")
        _require_positive(self.ingress_byte_limit, "ingress byte limit")
        _require_positive(self.ws_ingress_record_limit, "WS ingress record limit")
        _require_positive(self.ws_ingress_byte_limit, "WS ingress byte limit")
        _require_pct_watermarks(self.watermark_high_pct, self.watermark_low_pct)
        _require_positive(self.outbound_message_limit, "outbound message limit")
        _require_positive(self.outbound_byte_limit, "outbound byte limit")
        _require_non_negative(self.shutdown_drain_timeout_ms, "shutdown drain timeout")
        _require_positive(self.ws_max_frame_bytes, "WS frame limit")
        _require_positive(self.ws_max_message_bytes, "WS message limit")
        _require_non_negative(self.ping_interval_ms, "ping interval")
        _require_non_negative(self.pong_timeout_ms, "pong timeout")
        _require_non_negative(self.stats_interval_ms, "stats interval")
        # Mirrors the native builders: a single maximal payload must always
        # fit an empty ingress channel, or Backpressure would hold it forever.
        # Two header blocks: HTTP/2 caps the initial headers and the
        # trailers separately, and each may reach max_header_bytes.
        if self.ingress_byte_limit < self.max_body_bytes + 2 * self.max_header_bytes + 512:
            raise ValueError(
                "Web ingress_byte_limit must cover one maximal request "
                "(max_body_bytes + 2*max_header_bytes + 512)"
            )
        if self.ws_ingress_byte_limit < self.ws_max_message_bytes + 256:
            raise ValueError(
                "Web ws_ingress_byte_limit must cover one maximal message "
                "(ws_max_message_bytes + 256)"
            )
        _require_positive(self.h2_max_concurrent_streams, "h2_max_concurrent_streams")
        _require_positive(self.h2_initial_window_bytes, "h2_initial_window_bytes")
        # HTTP/2 windows and stream counts are 31-bit quantities (mirrors
        # the native builder).
        if self.h2_initial_window_bytes > 2**31 - 1:
            raise ValueError("Web h2_initial_window_bytes must be at most 2^31-1")
        if self.h2_max_concurrent_streams > 2**31 - 1:
            raise ValueError("Web h2_max_concurrent_streams must be at most 2^31-1")


@dataclass(frozen=True)
class WebClientConfig(CompoundScalar, namespace=_NAMESPACE):
    http_version_policy: WebHttpVersionPolicy = WebHttpVersionPolicy.AUTO
    max_connections_per_host: int = 6
    max_total_connections: int = 64
    connect_timeout_ms: int = 10_000
    request_timeout_ms: int = 30_000
    keep_alive_ms: int = 60_000
    follow_redirects: bool = True
    max_redirects: int = 5
    proxy: str = ""
    tls: Optional[TlsClientConfig] = None
    max_response_bytes: int = 16 * 1024 * 1024
    ingress_record_limit: int = 10_000
    ingress_byte_limit: int = 64 * 1024 * 1024
    watermark_high_pct: int = 80
    watermark_low_pct: int = 50
    outbound_record_limit: int = 10_000
    outbound_byte_limit: int = 64 * 1024 * 1024
    overflow: WebOverflowAction = WebOverflowAction.STAGE
    stage_overflow: WebOverflowAction = WebOverflowAction.FAIL
    shutdown_drain_timeout_ms: int = 5_000
    failure_policy: WebFailurePolicy = WebFailurePolicy.REPORT
    ws_ingress_record_limit: int = 10_000
    ws_ingress_byte_limit: int = 64 * 1024 * 1024
    ws_max_frame_bytes: int = 1024 * 1024
    ws_max_message_bytes: int = 16 * 1024 * 1024
    ping_interval_ms: int = 30_000
    pong_timeout_ms: int = 10_000
    stats_interval_ms: int = 0

    def __post_init__(self) -> None:
        _require_positive(self.max_connections_per_host, "max_connections_per_host")
        _require_positive(self.max_total_connections, "max_total_connections")
        _require_non_negative(self.connect_timeout_ms, "connect timeout")
        _require_non_negative(self.request_timeout_ms, "request timeout")
        _require_non_negative(self.keep_alive_ms, "keep-alive")
        _require_non_negative(self.max_redirects, "max_redirects")
        _require_positive(self.max_response_bytes, "max_response_bytes")
        # Mirrors the native builder: the 512-byte envelope overhead is
        # charged against this limit.
        if self.max_response_bytes < 1024:
            raise ValueError("Web max_response_bytes must be at least 1024")
        _require_positive(self.ingress_record_limit, "ingress record limit")
        _require_positive(self.ingress_byte_limit, "ingress byte limit")
        _require_pct_watermarks(self.watermark_high_pct, self.watermark_low_pct)
        _require_positive(self.outbound_record_limit, "outbound record limit")
        _require_positive(self.outbound_byte_limit, "outbound byte limit")
        _require_non_negative(self.shutdown_drain_timeout_ms, "shutdown drain timeout")
        _require_positive(self.ws_ingress_record_limit, "WS ingress record limit")
        _require_positive(self.ws_ingress_byte_limit, "WS ingress byte limit")
        _require_positive(self.ws_max_frame_bytes, "WS frame limit")
        _require_positive(self.ws_max_message_bytes, "WS message limit")
        # Mirrors the native builder: a single maximal message must always
        # fit an empty WS ingress channel.
        if self.ws_ingress_byte_limit < self.ws_max_message_bytes + 256:
            raise ValueError(
                "Web ws_ingress_byte_limit must cover one maximal message "
                "(ws_max_message_bytes + 256)"
            )
        _require_non_negative(self.ping_interval_ms, "ping interval")
        _require_non_negative(self.pong_timeout_ms, "pong timeout")
        _require_non_negative(self.stats_interval_ms, "stats interval")
        # The response reservation must fit the response channel, or no call
        # could ever be accepted (RFC 0024, flow control).
        if self.max_response_bytes > self.ingress_byte_limit:
            raise ValueError("Web max_response_bytes cannot exceed the ingress byte limit")


class WebRouteOutput(TimeSeriesSchema, namespace=_NAMESPACE):
    request: TS[HttpServerRequest]
    state: TS[WebRouteState]


class HttpRespondRequest(TimeSeriesSchema, namespace=_NAMESPACE):
    request_id: TS[int]
    response: TS[HttpResponse]


class WsRouteOutput(TimeSeriesSchema, namespace=_NAMESPACE):
    event: TS[WsEvent]
    frame: TS[WsInboundFrame]


class WsSendRequest(TimeSeriesSchema, namespace=_NAMESPACE):
    connection_id: TS[int]
    frame: TS[WsFrame]


class HttpClientCall(TimeSeriesSchema, namespace=_NAMESPACE):
    request: TS[HttpClientRequest]
    options: TS[HttpClientOptions]


# Transport failure is never disguised as an HTTP status: the failure arm is
# distinct from the response arm (RFC 0024).
class HttpCallResult(TimeSeriesSchema, namespace=_NAMESPACE):
    response: TS[HttpResponse]
    failure: TS[WebTransportError]


class WsClientOutput(TimeSeriesSchema, namespace=_NAMESPACE):
    event: TS[WsEvent]
    frame: TS[WsFrame]


class WsClientSendRequest(TimeSeriesSchema, namespace=_NAMESPACE):
    key: TS[WsClientKey]
    frame: TS[WsFrame]


# Configuration crosses a wiring-time scalar boundary rather than a TS
# annotation. Materialise its registered Bundle schema now so erased operator
# dispatch does not infer an arbitrary Python object (``Any``) at first use.
_WEB_SERVER_CONFIG_TS = TS[WebServerConfig]
_WEB_CLIENT_CONFIG_TS = TS[WebClientConfig]


def _http_serve_service(key: TS[WebRoute], path: str = "") -> TSB[WebRouteOutput]: ...


_http_serve_service.__name__ = "web_http_serve"
_web_http_serve_service = subscription_service(_http_serve_service)


def _http_respond_service(
    request: TSB[HttpRespondRequest], path: str = ""
) -> TS[WebDeliveryReport]: ...


_http_respond_service.__name__ = "web_http_respond"
_web_http_respond_service = request_reply_service(_http_respond_service)


def _ws_serve_service(key: TS[WebRoute], path: str = "") -> TSB[WsRouteOutput]: ...


_ws_serve_service.__name__ = "web_ws_serve"
_web_ws_serve_service = subscription_service(_ws_serve_service)


def _ws_send_service(
    request: TSB[WsSendRequest], path: str = ""
) -> TS[WebDeliveryReport]: ...


_ws_send_service.__name__ = "web_ws_send"
_web_ws_send_service = request_reply_service(_ws_send_service)


def web_server_events(path: str = "") -> TS[WebEvent]: ...


web_server_events = reference_service(web_server_events)


def web_server_stats(path: str = "") -> TS[WebServerStats]: ...


web_server_stats = reference_service(web_server_stats)


def _http_client_service(
    request: TSB[HttpClientCall], path: str = ""
) -> TSB[HttpCallResult]: ...


_http_client_service.__name__ = "web_http_client"
_web_http_client_service = request_reply_service(_http_client_service)


def _ws_client_service(key: TS[WsClientKey], path: str = "") -> TSB[WsClientOutput]: ...


_ws_client_service.__name__ = "web_ws_client"
_web_ws_client_service = subscription_service(_ws_client_service)


def _ws_client_send_service(
    request: TSB[WsClientSendRequest], path: str = ""
) -> TS[WebDeliveryReport]: ...


_ws_client_send_service.__name__ = "web_ws_client_send"
_web_ws_client_send_service = request_reply_service(_ws_client_send_service)


def web_client_events(path: str = "") -> TS[WebEvent]: ...


web_client_events = reference_service(web_client_events)


def web_client_stats(path: str = "") -> TS[WebClientStats]: ...


web_client_stats = reference_service(web_client_stats)


_register_server = operator_function("hgraph.web.register_server")
_register_client = operator_function("hgraph.web.register_client")


def register_web_server(config: WebServerConfig, path: str = "") -> None:
    """Register one lazy native web server implementation at ``path``.

    No socket is bound and no thread is started until the graph starts.
    """

    _register_server(config, path)


def register_web_client(config: WebClientConfig, path: str = "") -> None:
    """Register one lazy native web client implementation at ``path``."""

    _register_client(config, path)


def web_serve(route, path: str = ""):
    """Serve one HTTP route, ticking each request and the route's state."""

    if isinstance(route, WebRoute):
        route = const(route, tp=TS[WebRoute])
    return _web_http_serve_service(route, path=path)


def web_respond(request_id, response, path: str = ""):
    """Answer one served request, ticking its delivery report."""

    if isinstance(request_id, int):
        request_id = const(request_id, tp=TS[int])
    if isinstance(response, HttpResponse):
        response = const(response, tp=TS[HttpResponse])
    request = TSB[HttpRespondRequest].from_ts(request_id=request_id, response=response)
    return _web_http_respond_service(request, path=path)


def web_ws_serve(route, path: str = ""):
    """Serve one WebSocket route, ticking connection events and frames."""

    if isinstance(route, WebRoute):
        route = const(route, tp=TS[WebRoute])
    return _web_ws_serve_service(route, path=path)


def web_ws_send(connection_id, frame, path: str = ""):
    """Send one frame to a served WebSocket connection."""

    if isinstance(connection_id, int):
        connection_id = const(connection_id, tp=TS[int])
    if isinstance(frame, WsFrame):
        frame = const(frame, tp=TS[WsFrame])
    request = TSB[WsSendRequest].from_ts(connection_id=connection_id, frame=frame)
    return _web_ws_send_service(request, path=path)


def web_http_request(request, options=None, path: str = ""):
    """Make one client HTTP call, ticking the response or failure arm."""

    if isinstance(request, HttpClientRequest):
        request = const(request, tp=TS[HttpClientRequest])
    if options is None:
        options = const(HttpClientOptions(), tp=TS[HttpClientOptions])
    elif isinstance(options, HttpClientOptions):
        options = const(options, tp=TS[HttpClientOptions])
    call = TSB[HttpClientCall].from_ts(request=request, options=options)
    return _web_http_client_service(call, path=path)


def web_ws_connect(key, path: str = ""):
    """Connect one client WebSocket, ticking its events and frames."""

    if isinstance(key, WsClientKey):
        key = const(key, tp=TS[WsClientKey])
    return _web_ws_client_service(key, path=path)


def web_ws_client_send(key, frame, path: str = ""):
    """Send one frame over a client WebSocket connection."""

    if isinstance(key, WsClientKey):
        key = const(key, tp=TS[WsClientKey])
    if isinstance(frame, WsFrame):
        frame = const(frame, tp=TS[WsFrame])
    request = TSB[WsClientSendRequest].from_ts(key=key, frame=frame)
    return _web_ws_client_send_service(request, path=path)


__all__ = [
    # Enums
    "HttpMethod",
    "WebClientVerify",
    "WebDeliveryStatus",
    "WebFailurePolicy",
    "WebHttpVersionPolicy",
    "WebInboundOverflow",
    "WebOverflowAction",
    "WebRouteState",
    "WebSeverity",
    "WebSlowConsumerPolicy",
    "WebTlsVersion",
    "WsConnectionState",
    "WsFrameKind",
    # Values
    "HttpClientOptions",
    "HttpClientRequest",
    "HttpRequest",
    "HttpResponse",
    "HttpServerRequest",
    "TlsClientConfig",
    "TlsServerConfig",
    "WebClientConfig",
    "WebClientStats",
    "WebDeliveryReport",
    "WebEvent",
    "WebHeader",
    "WebParam",
    "WebPeer",
    "WebRoute",
    "WebServerConfig",
    "WebServerStats",
    "WebTransportError",
    "WsClientKey",
    "WsEvent",
    "WsFrame",
    "WsInboundFrame",
    # Time-series collections
    "HttpCallResult",
    "HttpClientCall",
    "HttpRespondRequest",
    "WebRouteOutput",
    "WsClientOutput",
    "WsClientSendRequest",
    "WsRouteOutput",
    "WsSendRequest",
    # Registration and wiring
    "register_web_client",
    "register_web_server",
    "web_client_events",
    "web_client_stats",
    "web_http_request",
    "web_respond",
    "web_serve",
    "web_server_events",
    "web_server_stats",
    "web_ws_client_send",
    "web_ws_connect",
    "web_ws_send",
    "web_ws_serve",
]

# The RFC 0024 authoring decorators, the wiring-supplied authenticator, and the
# codec helpers build on the wiring helpers above, so they are imported last.
from ._endpoints import (  # noqa: E402
    AuthResult,
    binary_frame,
    http_endpoint,
    json_response,
    request_json,
    text_frame,
    ws_binary,
    ws_endpoint,
    ws_json,
    ws_text,
)

__all__ += [
    # Authoring
    "AuthResult",
    "http_endpoint",
    "ws_endpoint",
    # Codecs
    "binary_frame",
    "json_response",
    "request_json",
    "text_frame",
    "ws_binary",
    "ws_json",
    "ws_text",
]
