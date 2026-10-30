from __future__ import annotations

from datetime import timedelta
from pathlib import Path
import importlib.util
import subprocess
import sys

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

from . import _loopback_harness as harness


@pytest.mark.parametrize("persistence_first", [True, False])
def test_verified_tls_after_other_native_crypto_libraries(persistence_first):
    if importlib.util.find_spec("hgraph_persistence") is None:
        pytest.skip("the import-order regression requires the persistence wheel")
    imports = ["hgraph_persistence", "hgraph_web"]
    if not persistence_first:
        imports.reverse()
    result = subprocess.run(
        [sys.executable, "-c", """
import importlib
import sys
import pytest
for name in sys.argv[1:3]:
    importlib.import_module(name)
raise SystemExit(pytest.main([
    sys.argv[3], '-q', '-k', 'test_verified_tls_ca_selection',
]))
""", *imports, str(Path(__file__).resolve())],
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "trust,valid_environment,expect_success",
    [
        ("environment", True, True),
        ("environment", False, False),
        ("path", False, True),
        ("pem", False, True),
    ],
)
def test_verified_tls_ca_selection(
    monkeypatch, trust, valid_environment, expect_success
):
    fixtures = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
    cert = fixtures / "localhost-cert.pem"
    monkeypatch.setenv(
        "SSL_CERT_FILE", str(cert if valid_environment else fixtures / "missing.pem")
    )
    observed, requested, failures = [], [], []
    tls = None
    if trust == "path":
        tls = web.TlsClientConfig(ca_path=str(cert))
    elif trust == "pem":
        tls = web.TlsClientConfig(ca_pem=cert.read_text())
    request = harness.port_triggered_request(
        requested,
        lambda port: web.HttpClientRequest(
            web.HttpMethod.GET, f"https://127.0.0.1:{port}/cert"
        ),
    )
    capture, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        web.register_web_server(
            web.WebServerConfig(
                port=0,
                stats_interval_ms=10,
                tls=web.TlsServerConfig(
                    cert_path=str(cert), key_path=str(fixtures / "localhost-key.pem")
                ),
                static_files=(web.WebStaticFile("/cert", str(cert)),),
            ),
            path="site",
        )
        web.register_web_client(web.WebClientConfig(tls=tls), path="api")
        result = web.web_http_request(
            request(web.web_server_stats(path="site")), path="api"
        )
        capture(result["response"])
        capture_failure(result["failure"])

    start = hg.utc_now()
    hg.eval_node(
        app,
        __run_mode__=hg.EvaluationMode.REAL_TIME,
        __start_time__=start,
        __end_time__=start + timedelta(seconds=5),
        __elide__=True,
    )
    assert len(requested) == 1
    if expect_success:
        assert failures == []
        assert len(observed) == 1 and observed[0].status == 200
    else:
        assert observed == []
        assert len(failures) == 1
        assert failures[0].error_code == 77  # CURLE_SSL_CACERT_BADFILE


def _header(response, name):
    """One response header by case-insensitive name.

    HTTP field names are case-insensitive (RFC 9110 5.1), and this server
    demonstrates why that matters: graph responses are written through
    ``H2Headers`` as ``content-type``, while the static-file path goes out via
    Beast's HTTP/1.1 serialiser as ``Content-Type``. Pinning one spelling
    tests whichever serialiser happens to answer, not the behaviour. Matches
    the lookup already used in test_endpoints.py and hgraph_web.compat.
    """
    return next(
        (header.value for header in response.headers if header.name.lower() == name),
        None,
    )


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
    assert _header(observed[0], "content-type") == "text/plain"


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
    assert _header(observed[0], "content-type") == "image/x-icon"


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
    assert _header(observed[0], "content-type") == "text/javascript; charset=utf-8"


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
