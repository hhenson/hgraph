from __future__ import annotations

from datetime import timedelta

import pytest

import hgraph as hg

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

import _loopback_harness as harness


def test_a_python_graph_serves_and_calls_itself_over_a_real_socket() -> None:
    """The full loopback: this graph is both the server and the client.

    The listening port is chosen by the OS (``port=0``) and discovered from
    the server's own stats stream, so the client request cannot be wired
    until the transport is actually serving.
    """

    observed = []
    requested = []
    failures = []

    @hg.compute_node
    def request_id(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request.value.request_id

    @hg.compute_node
    def echo(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        inbound = request.value.request
        name = next(
            (param.value for param in inbound.path_params if param.name == "name"),
            "",
        )
        return web.HttpResponse(
            200,
            headers=(web.WebHeader("content-type", "text/plain"),),
            body=f"hello {name}".encode(),
        )

    client_request = harness.port_triggered_request(
        requested,
        lambda port: web.HttpClientRequest(
            web.HttpMethod.GET, f"http://127.0.0.1:{port}/echo/py"
        ),
    )
    capture, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        harness.register_loopback()

        served = web.web_serve(
            web.WebRoute(web.HttpMethod.GET, "/echo/{name}"), path="site"
        )
        web.web_respond(
            request_id(served["request"]), echo(served["request"]), path="site"
        )

        result = web.web_http_request(
            client_request(web.web_server_stats(path="site")), path="api"
        )
        capture(result["response"])
        capture_failure(result["failure"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert failures == []
    assert len(requested) == 1 and requested[0] > 0
    assert len(observed) == 1
    assert observed[0].status == 200
    assert observed[0].body == b"hello py"
    assert ("content-type", "text/plain") in tuple(
        (header.name, header.value) for header in observed[0].headers
    )


def test_a_static_file_mount_is_served_by_the_transport(tmp_path) -> None:
    observed = []
    requested = []
    failures = []

    favicon = tmp_path / "favicon.ico"
    favicon.write_bytes(b"\x00ICO")

    client_request = harness.port_triggered_request(
        requested,
        lambda port: web.HttpClientRequest(
            web.HttpMethod.GET, f"http://127.0.0.1:{port}/favicon.ico"
        ),
    )
    capture, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        harness.register_loopback(
            web.WebServerConfig(
                port=0,
                stats_interval_ms=50,
                static_files=(
                    web.WebStaticFile("/favicon.ico", str(favicon)),
                ),
            )
        )
        result = web.web_http_request(
            client_request(web.web_server_stats(path="site")), path="api"
        )
        capture(result["response"])
        capture_failure(result["failure"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert failures == []
    assert len(observed) == 1
    assert observed[0].status == 200
    assert observed[0].body == b"\x00ICO"
    assert ("content-type", "image/x-icon") in tuple(
        (header.name, header.value) for header in observed[0].headers
    )


def test_a_static_directory_mount_is_served_by_the_transport(tmp_path) -> None:
    observed = []
    requested = []
    failures = []

    assets = tmp_path / "assets" / "nested"
    assets.mkdir(parents=True)
    app_js = assets / "app.js"
    app_js.write_bytes(b"console.log('ok');")

    client_request = harness.port_triggered_request(
        requested,
        lambda port: web.HttpClientRequest(
            web.HttpMethod.GET, f"http://127.0.0.1:{port}/assets/nested/app.js"
        ),
    )
    capture, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        harness.register_loopback(
            web.WebServerConfig(
                port=0,
                stats_interval_ms=50,
                static_directories=(
                    web.WebStaticDirectory("/assets", str(tmp_path / "assets")),
                ),
            )
        )
        result = web.web_http_request(
            client_request(web.web_server_stats(path="site")), path="api"
        )
        capture(result["response"])
        capture_failure(result["failure"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert failures == []
    assert len(observed) == 1
    assert observed[0].status == 200
    assert observed[0].body == b"console.log('ok');"
    assert ("content-type", "text/javascript; charset=utf-8") in tuple(
        (header.name, header.value) for header in observed[0].headers
    )


def test_a_refused_connection_arrives_on_the_failure_arm() -> None:
    """Transport failure is never disguised as an HTTP status (RFC 0024)."""

    observed = []
    responses = []

    @hg.sink_node
    def capture_failure(
        failure: hg.TS[web.WebTransportError],
        _api: hg.EvaluationEngineApi = None,
    ):
        observed.append(failure.value)
        _api.request_engine_stop()

    @hg.sink_node
    def capture_response(
        response: hg.TS[web.HttpResponse],
        _api: hg.EvaluationEngineApi = None,
    ):
        responses.append(response.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        web.register_web_client(
            web.WebClientConfig(connect_timeout_ms=1_000), path="api"
        )
        result = web.web_http_request(
            web.HttpClientRequest(web.HttpMethod.GET, "http://127.0.0.1:1/nothing"),
            path="api",
        )
        capture_failure(result["failure"])
        capture_response(result["response"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert responses == []
    assert len(observed) == 1
    assert observed[0].error_code != 0
    assert observed[0].message
