from __future__ import annotations

import pytest

import _hgraph
import hgraph as hg
from hgraph.test import use_wiring

try:
    import hgraph_web as web
except ImportError as error:  # pragma: no cover - exercised without the wheel
    # Both flavours matter: the distribution may be absent entirely, or its
    # native module may be present but unloadable. pytest.importorskip only
    # covers the first of those from pytest 9.1 on.
    pytest.skip(
        f"the hgraph-web native module is not importable: {error}",
        allow_module_level=True,
    )


def test_public_values_bind_the_native_web_schemas() -> None:
    # Every schema resolves through the registry the native module already
    # populated at import; a divergent field list or order would be rejected
    # as a re-registration of a named bundle with a different schema.
    for name in (
        "WebHeader",
        "WebParam",
        "WebPeer",
        "WebRoute",
        "HttpRequest",
        "HttpServerRequest",
        "HttpResponse",
        "WebTransportError",
        "HttpClientRequest",
        "HttpClientOptions",
        "WsFrame",
        "WsInboundFrame",
        "WsEvent",
        "WsClientKey",
        "WebDeliveryReport",
        "WebEvent",
        "WebServerStats",
        "WebClientStats",
        "TlsServerConfig",
        "TlsClientConfig",
        "WebServerConfig",
        "WebClientConfig",
    ):
        assert issubclass(getattr(web, name), hg.CompoundScalar)
        assert getattr(web, name).__compound_namespace__ == "hgraph.web"
        assert hg.TS[getattr(web, name)] is not None


def test_a_divergent_python_schema_is_rejected_by_the_registry() -> None:
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class WebRoute(hg.CompoundScalar, namespace="hgraph.web"):
        method: web.HttpMethod
        pattern: str

    with pytest.raises(ValueError, match="already registered with a different schema"):
        hg.TS[WebRoute]


def test_time_series_collections_use_the_native_named_tsb_schemas() -> None:
    for name in (
        "WebRouteOutput",
        "HttpRespondRequest",
        "WsRouteOutput",
        "WsSendRequest",
        "HttpClientCall",
        "HttpCallResult",
        "WsClientOutput",
        "WsClientSendRequest",
    ):
        schema = getattr(web, name)
        assert issubclass(schema, hg.TimeSeriesSchema)
        assert schema.__time_series_namespace__ == "hgraph.web"

    assert hg.TSB[web.WebRouteOutput].handle == _hgraph.tsb(
        "hgraph.web::WebRouteOutput",
        [
            ("request", hg.TS[web.HttpServerRequest].handle),
            ("state", hg.TS[web.WebRouteState].handle),
        ],
    )
    assert hg.TSB[web.HttpRespondRequest].handle == _hgraph.tsb(
        "hgraph.web::HttpRespondRequest",
        [
            ("request_id", hg.TS[int].handle),
            ("response", hg.TS[web.HttpResponse].handle),
        ],
    )
    assert hg.TSB[web.HttpCallResult].handle == _hgraph.tsb(
        "hgraph.web::HttpCallResult",
        [
            ("response", hg.TS[web.HttpResponse].handle),
            ("failure", hg.TS[web.WebTransportError].handle),
        ],
    )
    assert hg.TSB[web.WsClientSendRequest].handle == _hgraph.tsb(
        "hgraph.web::WsClientSendRequest",
        [
            ("key", hg.TS[web.WsClientKey].handle),
            ("frame", hg.TS[web.WsFrame].handle),
        ],
    )


def test_service_stubs_carry_the_native_descriptor_names() -> None:
    assert web._web_http_serve_service.__name__ == "web_http_serve"
    assert web._web_http_respond_service.__name__ == "web_http_respond"
    assert web._web_ws_serve_service.__name__ == "web_ws_serve"
    assert web._web_ws_send_service.__name__ == "web_ws_send"
    assert web._web_http_client_service.__name__ == "web_http_client"
    assert web._web_ws_client_service.__name__ == "web_ws_client"
    assert web._web_ws_client_send_service.__name__ == "web_ws_client_send"
    assert web.web_server_events.__name__ == "web_server_events"
    assert web.web_server_stats.__name__ == "web_server_stats"
    assert web.web_client_events.__name__ == "web_client_events"
    assert web.web_client_stats.__name__ == "web_client_stats"

    assert web._web_http_serve_service.flavour == "subscription"
    assert web._web_ws_serve_service.flavour == "subscription"
    assert web._web_ws_client_service.flavour == "subscription"
    assert web._web_http_respond_service.flavour == "request_reply"
    assert web._web_ws_send_service.flavour == "request_reply"
    assert web._web_http_client_service.flavour == "request_reply"
    assert web._web_ws_client_send_service.flavour == "request_reply"
    assert web.web_server_events.flavour == "reference"
    assert web.web_server_stats.flavour == "reference"
    assert web.web_client_events.flavour == "reference"
    assert web.web_client_stats.flavour == "reference"


def test_config_defaults_match_the_native_builders() -> None:
    # The C++ builders (ServerConfigBuilder / ClientConfigBuilder member
    # initializers in value_builders.h) are the contract these mirror.
    server = web.WebServerConfig()
    assert (server.bind_address, server.port, server.tls) == ("0.0.0.0", 0, None)
    assert (server.io_threads, server.max_connections) == (1, 10_000)
    assert (server.max_header_bytes, server.max_body_bytes) == (
        64 * 1024,
        16 * 1024 * 1024,
    )
    assert (
        server.request_timeout_ms,
        server.idle_timeout_ms,
        server.keep_alive_timeout_ms,
    ) == (30_000, 60_000, 15_000)
    assert server.bind_deferred is False
    assert (server.ingress_record_limit, server.ingress_byte_limit) == (
        10_000,
        64 * 1024 * 1024,
    )
    assert (server.ws_ingress_record_limit, server.ws_ingress_byte_limit) == (
        10_000,
        64 * 1024 * 1024,
    )
    assert (server.watermark_high_pct, server.watermark_low_pct) == (80, 50)
    assert server.inbound_overflow == web.WebInboundOverflow.BACKPRESSURE
    assert (server.outbound_message_limit, server.outbound_byte_limit) == (
        1_000,
        16 * 1024 * 1024,
    )
    assert server.slow_consumer_policy == web.WebSlowConsumerPolicy.CLOSE
    assert server.failure_policy == web.WebFailurePolicy.REPORT
    assert server.shutdown_drain_timeout_ms == 5_000
    assert (server.ws_max_frame_bytes, server.ws_max_message_bytes) == (
        1024 * 1024,
        16 * 1024 * 1024,
    )
    assert (server.ping_interval_ms, server.pong_timeout_ms) == (30_000, 10_000)
    assert server.stats_interval_ms == 0
    assert (server.h2_max_concurrent_streams, server.h2_initial_window_bytes) == (
        100,
        1024 * 1024,
    )

    client = web.WebClientConfig()
    assert client.http_version_policy == web.WebHttpVersionPolicy.AUTO
    assert (client.max_connections_per_host, client.max_total_connections) == (6, 64)
    assert (
        client.connect_timeout_ms,
        client.request_timeout_ms,
        client.keep_alive_ms,
    ) == (10_000, 30_000, 60_000)
    assert (client.follow_redirects, client.max_redirects) == (True, 5)
    assert (client.proxy, client.tls) == ("", None)
    assert client.max_response_bytes == 16 * 1024 * 1024
    assert (client.ingress_record_limit, client.ingress_byte_limit) == (
        10_000,
        64 * 1024 * 1024,
    )
    assert (client.watermark_high_pct, client.watermark_low_pct) == (80, 50)
    assert (client.outbound_record_limit, client.outbound_byte_limit) == (
        10_000,
        64 * 1024 * 1024,
    )
    assert client.overflow == web.WebOverflowAction.STAGE
    assert client.stage_overflow == web.WebOverflowAction.FAIL
    assert client.shutdown_drain_timeout_ms == 5_000
    assert client.failure_policy == web.WebFailurePolicy.REPORT
    assert (client.ws_ingress_record_limit, client.ws_ingress_byte_limit) == (
        10_000,
        64 * 1024 * 1024,
    )
    assert (client.ws_max_frame_bytes, client.ws_max_message_bytes) == (
        1024 * 1024,
        16 * 1024 * 1024,
    )
    assert (client.ping_interval_ms, client.pong_timeout_ms) == (30_000, 10_000)
    assert client.stats_interval_ms == 0

    options = web.HttpClientOptions()
    assert (options.connect_timeout_ms, options.request_timeout_ms) == (10_000, 30_000)
    assert (options.follow_redirects, options.max_redirects) == (True, 5)
    assert options.http_version == web.WebHttpVersionPolicy.AUTO

    # An unset server ALPN reaches the transport as http/1.1 alone, as the
    # native builder writes it.
    assert web.TlsServerConfig(cert_path="c", key_path="k").alpn == ("http/1.1",)


@pytest.mark.parametrize(
    ("factory", "error"),
    [
        (lambda: web.WebServerConfig(port=65_536), "port must be 0..65535"),
        (lambda: web.WebServerConfig(port=-1), "port must be 0..65535"),
        (lambda: web.WebServerConfig(bind_address=""), "bind address cannot be empty"),
        (lambda: web.WebServerConfig(io_threads=0), "io_threads must be positive"),
        (
            lambda: web.WebServerConfig(watermark_high_pct=50, watermark_low_pct=80),
            "0 < low < high < 100",
        ),
        (
            lambda: web.WebServerConfig(watermark_high_pct=100, watermark_low_pct=50),
            "0 < low < high < 100",
        ),
        (
            lambda: web.WebServerConfig(watermark_high_pct=80, watermark_low_pct=0),
            "0 < low < high < 100",
        ),
        (
            lambda: web.WebServerConfig(request_timeout_ms=-1),
            "request timeout cannot be negative",
        ),
        (
            lambda: web.TlsServerConfig(
                cert_path="cert.pem", key_path="key.pem", alpn=("spdy/3",)
            ),
            'accepts only "h2" and "http/1.1"',
        ),
        (
            lambda: web.TlsServerConfig(
                cert_path="cert.pem", cert_pem="-----BEGIN", key_path="key.pem"
            ),
            "path or inline PEM, not both",
        ),
        (
            lambda: web.TlsServerConfig(key_path="key.pem"),
            "requires a certificate chain",
        ),
        (lambda: web.TlsServerConfig(cert_path="cert.pem"), "requires a private key"),
        (
            lambda: web.TlsServerConfig(
                cert_path="cert.pem",
                key_path="key.pem",
                client_verify=web.WebClientVerify.REQUIRED,
            ),
            "verification requires a CA",
        ),
        (
            lambda: web.TlsClientConfig(cert_path="cert.pem"),
            "mTLS requires both a certificate and a private key",
        ),
        (
            lambda: web.TlsClientConfig(key_pem="-----BEGIN"),
            "mTLS requires both a certificate and a private key",
        ),
        (
            lambda: web.TlsClientConfig(ca_path="ca.pem", ca_pem="-----BEGIN"),
            "path or inline PEM, not both",
        ),
        (
            lambda: web.WebClientConfig(
                max_response_bytes=1024, ingress_byte_limit=512
            ),
            "max_response_bytes cannot exceed the ingress byte limit",
        ),
        (
            lambda: web.WebClientConfig(max_connections_per_host=0),
            "max_connections_per_host must be positive",
        ),
        (
            lambda: web.WebClientConfig(max_redirects=-1),
            "max_redirects cannot be negative",
        ),
        (lambda: web.WebRoute(web.HttpMethod.GET, "echo"), "must start with '/'"),
        (lambda: web.WebRoute(web.HttpMethod.GET, ""), "must start with '/'"),
        (lambda: web.HttpResponse(99), "status must be 100..599"),
        (lambda: web.HttpResponse(600), "status must be 100..599"),
        (lambda: web.WebHeader(""), "header names cannot be empty"),
        (lambda: web.WebParam("", "value"), "parameter names cannot be empty"),
        (
            lambda: web.HttpClientRequest(web.HttpMethod.GET, ""),
            "client requests require a URL",
        ),
        (lambda: web.WsClientKey(""), "client keys require a URL"),
        (
            lambda: web.WsFrame.close_frame(999),
            "close codes must be 1000..4999",
        ),
        (
            lambda: web.WsFrame.close_frame(5000),
            "close codes must be 1000..4999",
        ),
        (
            lambda: web.HttpClientOptions(connect_timeout_ms=-1),
            "connect timeout cannot be negative",
        ),
    ],
)
def test_invalid_public_values_are_rejected_at_construction(factory, error) -> None:
    with pytest.raises(ValueError, match=error):
        factory()


def test_ws_frame_helpers_mirror_the_native_makers() -> None:
    text = web.WsFrame.text_frame("hello")
    assert (text.kind, text.text, text.data) == (web.WsFrameKind.TEXT, "hello", None)

    binary = web.WsFrame.binary_frame(b"\x00\x01")
    assert (binary.kind, binary.data, binary.text) == (
        web.WsFrameKind.BINARY,
        b"\x00\x01",
        None,
    )

    close = web.WsFrame.close_frame(1001, "going away")
    assert (close.kind, close.close_code, close.close_reason) == (
        web.WsFrameKind.CLOSE,
        1001,
        "going away",
    )


def test_ordered_duplicate_headers_and_params_are_preserved() -> None:
    response = web.HttpResponse(
        200,
        headers=(
            web.WebHeader("set-cookie", "a=1"),
            web.WebHeader("set-cookie", "b=2"),
        ),
    )

    assert tuple(header.value for header in response.headers) == ("a=1", "b=2")


def test_every_service_interface_wires_against_one_registered_path() -> None:
    @hg.compute_node
    def request_id(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request.value.request_id

    @hg.compute_node
    def respond(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        del request
        return web.HttpResponse(200)

    wiring = _hgraph.Wiring()
    key = web.WsClientKey("ws://127.0.0.1:9/live")

    with use_wiring(wiring):
        web.register_web_server(web.WebServerConfig(port=0), path="site")
        web.register_web_client(web.WebClientConfig(), path="api")

        served = web.web_serve(
            web.WebRoute(web.HttpMethod.GET, "/orders/{id}"), path="site"
        )
        delivery = web.web_respond(
            request_id(served["request"]), respond(served["request"]), path="site"
        )
        upgraded = web.web_ws_serve(
            web.WebRoute(web.HttpMethod.GET, "/live", upgrade=True), path="site"
        )
        frame_delivery = web.web_ws_send(
            1, web.WsFrame.text_frame("hello"), path="site"
        )
        events = web.web_server_events(path="site")
        stats = web.web_server_stats(path="site")

        result = web.web_http_request(
            web.HttpClientRequest(web.HttpMethod.GET, "http://127.0.0.1:9/orders/1"),
            path="api",
        )
        connected = web.web_ws_connect(key, path="api")
        client_delivery = web.web_ws_client_send(
            key, web.WsFrame.text_frame("hello"), path="api"
        )
        client_events = web.web_client_events(path="api")
        client_stats = web.web_client_stats(path="api")

    for port in (
        served,
        delivery,
        upgraded,
        frame_delivery,
        events,
        stats,
        result,
        connected,
        client_delivery,
        client_events,
        client_stats,
    ):
        assert port is not None

    wiring.build_services()


def test_a_dynamic_route_and_an_explicit_options_arm_wire_the_same_way() -> None:
    wiring = _hgraph.Wiring()

    with use_wiring(wiring):
        web.register_web_server(web.WebServerConfig(port=0), path="site")
        web.register_web_client(web.WebClientConfig(), path="api")

        dynamic_route = hg.const(
            web.WebRoute(web.HttpMethod.POST, "/orders"), tp=hg.TS[web.WebRoute]
        )
        served = web.web_serve(dynamic_route, path="site")
        result = web.web_http_request(
            hg.const(
                web.HttpClientRequest(web.HttpMethod.GET, "http://127.0.0.1:9/"),
                tp=hg.TS[web.HttpClientRequest],
            ),
            web.HttpClientOptions(request_timeout_ms=1_000),
            path="api",
        )

    assert served is not None
    assert result is not None
    wiring.build_services()
