"""Parity smoke tests for the released tornado server surface on the native
transport.

Every test is a single real-time graph that is both the compatibility server
and its own client: the compat handler answers a request issued by
``web_http_request`` in the same graph, so what is asserted is what actually
crossed a socket.  The client waits for the server's own stats stream to report
a listening port before it sends, which is why the server is registered with a
non-zero ``stats_interval_ms``.
"""

import socket
from datetime import timedelta

import pytest

import hgraph as hg

try:
    import hgraph_web as web
    from hgraph_web import compat
except ImportError as error:  # pragma: no cover - exercised without the wheel
    pytest.skip(
        f"the hgraph-web native module is not importable: {error}",
        allow_module_level=True,
    )


@pytest.fixture(autouse=True)
def isolated_handler_registry(monkeypatch):
    """The released handler registries are module-global and are never emptied.

    A handler declared by one test would therefore be auto-wired into every
    later test's graph; the registries are restored per test instead.
    """
    monkeypatch.setattr(compat, "_HTTP_SERVER_HANDLERS", {})
    monkeypatch.setattr(compat, "_WEBSOCKET_SERVER_HANDLERS", {})


def _server_config():
    """The bound port is discovered from stats, so the interval must be short.

    The drains keep teardown from dominating the suite: the transport waits its
    full drain timeout at shutdown even with nothing in flight.
    """

    return web.WebServerConfig(stats_interval_ms=50, shutdown_drain_timeout_ms=250)


def _client_config():
    return web.WebClientConfig(shutdown_drain_timeout_ms=250)


@pytest.fixture
def free_tcp_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _call(port, target, *, method=None, headers=(), body=b"", extra=None):
    """Register the compat server on ``port`` and make one client call to it.

    ``extra`` is wired inside the graph, for handlers that the released adaptor
    requires to be wired explicitly.
    """
    method = web.HttpMethod.GET if method is None else method
    service_path = compat.compat_service_path(port)
    observed = []
    failures = []
    sent = []

    @hg.compute_node
    def client_request(
        stats: hg.TS[web.WebServerStats],
    ) -> hg.TS[web.HttpClientRequest]:
        if stats.value.listening_port == 0 or sent:
            return None
        sent.append(True)
        return web.HttpClientRequest(
            method,
            f"http://127.0.0.1:{port}{target}",
            headers=tuple(headers),
            body=body,
        )

    @hg.sink_node
    def capture(
        response: hg.TS[web.HttpResponse], _api: hg.EvaluationEngineApi = None
    ):
        observed.append(response.value)
        _api.request_engine_stop()

    @hg.sink_node
    def capture_failure(
        failure: hg.TS[web.WebTransportError], _api: hg.EvaluationEngineApi = None
    ):
        failures.append(failure.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        compat.register_http_server_adaptor(port, _server_config())
        web.register_web_client(_client_config(), path="api")
        if extra is not None:
            extra()
        result = web.web_http_request(
            client_request(web.web_server_stats(path=service_path)), path="api"
        )
        capture(result["response"])
        capture_failure(result["failure"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert failures == [], f"transport failure: {failures}"
    assert len(observed) == 1, f"expected one response, saw {observed}"
    return observed[0]


def _header(response, name):
    return next(
        (
            header.value
            for header in response.headers
            if header.name.lower() == name.lower()
        ),
        None,
    )


def test_a_single_request_handler_answers_a_real_http_get(free_tcp_port):
    received = []

    @compat.http_server_handler(url=f"/compat-echo-{free_tcp_port}/(.*)")
    @hg.compute_node
    def handler(request: hg.TS[compat.HttpRequest]) -> hg.TS[compat.HttpResponse]:
        received.append(request.value)
        return compat.HttpResponse(
            status_code=200,
            headers={"Content-Type": "text/plain"},
            body=f"handled:{request.value.url_parsed_args[0]}".encode(),
        )

    response = _call(free_tcp_port, f"/compat-echo-{free_tcp_port}/item")

    assert response.status == 200
    assert response.body == b"handled:item"
    assert _header(response, "content-type") == "text/plain"
    assert len(received) == 1
    assert isinstance(received[0], compat.HttpGetRequest)
    # ``url`` is the registered route pattern, never the requested path.
    assert received[0].url == f"/compat-echo-{free_tcp_port}/(.*)"
    assert received[0].auth == ("Anonymous", "Anonymous")


def test_a_positional_capture_arrives_in_url_parsed_args(free_tcp_port):
    received = []

    @compat.http_server_handler(url=f"/compat-greet-{free_tcp_port}/(\\w+)/(\\w+)")
    @hg.compute_node
    def handler(request: hg.TS[compat.HttpRequest]) -> hg.TS[compat.HttpResponse]:
        received.append(request.value)
        return compat.HttpResponse(
            status_code=200, body=":".join(request.value.url_parsed_args).encode()
        )

    response = _call(free_tcp_port, f"/compat-greet-{free_tcp_port}/hello/world")

    assert response.status == 200
    assert response.body == b"hello:world"
    assert received[0].url_parsed_args == ("hello", "world")
    # No content type set by the handler: tornado's RequestHandler default.
    assert _header(response, "content-type") == "text/html; charset=UTF-8"


def test_duplicate_request_headers_collapse_to_one_entry(free_tcp_port):
    """The released header collapse is the contract, defect and all."""
    received = []

    @compat.http_server_handler(url=f"/compat-headers-{free_tcp_port}")
    @hg.compute_node
    def handler(request: hg.TS[compat.HttpRequest]) -> hg.TS[compat.HttpResponse]:
        received.append(request.value)
        return compat.HttpResponse(status_code=200)

    _call(
        free_tcp_port,
        f"/compat-headers-{free_tcp_port}?tag=a&tag=b&flag",
        headers=(
            web.WebHeader("x-hgraph-test", "first"),
            web.WebHeader("x-hgraph-test", "second"),
        ),
    )

    headers = received[0].headers
    matching = [name for name in headers if name.lower() == "x-hgraph-test"]
    assert matching == ["X-Hgraph-Test"], f"expected one entry, saw {matching}"
    assert headers["X-Hgraph-Test"] == "first,second"
    # Multi-valued query parameters are concatenated with no separator.
    assert received[0].query == {"tag": "ab", "flag": ""}


def test_a_keyed_batch_handler_receives_and_answers_by_request_id(free_tcp_port):
    received = []

    @compat.http_server_handler(url=f"/compat-batch-{free_tcp_port}")
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[compat.HttpRequest]],
    ) -> hg.TSD[int, hg.TS[compat.HttpResponse]]:
        responses = {}
        for request_id, value in request.modified_items():
            received.append(value.value)
            responses[request_id] = compat.HttpResponse(
                status_code=201, body=f"batched:{len(received)}".encode()
            )
        return responses

    response = _call(
        free_tcp_port,
        f"/compat-batch-{free_tcp_port}",
        method=web.HttpMethod.POST,
        body=b"payload",
    )

    assert response.status == 201
    assert response.body == b"batched:1"
    assert isinstance(received[0], compat.HttpPostRequest)
    assert received[0].body == "payload"


def test_a_keyed_auxiliary_output_stays_observable(free_tcp_port):
    audits = []

    class HandlerOutput(hg.TimeSeriesSchema):
        response: hg.TS[compat.HttpResponse]
        audit: hg.TS[str]

    @compat.http_server_handler(url=f"/compat-aux-{free_tcp_port}")
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[compat.HttpRequest]],
    ) -> hg.TSD[int, hg.TSB[HandlerOutput]]:
        return {
            request_id: {
                "response": compat.HttpResponse(
                    status_code=202, body=f"handled:{value.value.url}".encode()
                ),
                "audit": type(value.value).__name__,
            }
            for request_id, value in request.modified_items()
        }

    @hg.sink_node
    def capture_audit(audit: hg.TSD[int, hg.TS[str]]) -> None:
        audits.extend(value.value for _, value in audit.modified_items())

    def wire_handler():
        # Wiring a handler twice in one graph is idempotent, as released.
        first = handler()
        assert handler() is first
        capture_audit(first.audit)

    # An auxiliary-output handler is never auto-wired: the released adaptor
    # requires it to be called so the rest of its output stays observable.
    assert handler.auto_wire is False
    response = _call(
        free_tcp_port, f"/compat-aux-{free_tcp_port}", extra=wire_handler
    )

    assert response.status == 202
    assert response.body == f"handled:/compat-aux-{free_tcp_port}".encode()
    assert audits == ["HttpGetRequest"]


def test_an_explicit_adaptor_client_serves_its_own_route(free_tcp_port):
    """The advanced tier: a route wired through ``http_server_adaptor``.

    The client is discovered by the registered implementation, which serves the
    route from the same compatibility server the handlers use.
    """
    route = f"/compat-adaptor-{free_tcp_port}/(.*)"
    received = []

    @hg.compute_node
    def handle(
        requests: hg.TSD[int, hg.TS[compat.HttpRequest]],
    ) -> hg.TSD[int, hg.TS[compat.HttpResponse]]:
        responses = {}
        for request_id, request in requests.modified_items():
            received.append(request.value)
            responses[request_id] = compat.HttpResponse(
                status_code=203, body=request.value.url_parsed_args[0].encode()
            )
        return responses

    def wire_adaptor():
        feedback = hg.feedback(hg.TSD[int, hg.TS[compat.HttpResponse]])
        feedback(handle(compat.http_server_adaptor(feedback(), path=route)))

    response = _call(
        free_tcp_port,
        f"/compat-adaptor-{free_tcp_port}/widget",
        extra=wire_adaptor,
    )

    assert response.status == 203
    assert response.body == b"widget"
    assert [type(value) for value in received] == [compat.HttpGetRequest]
    assert received[0].url == route


def test_an_unimplemented_method_is_answered_405(free_tcp_port):
    called = []

    @compat.http_server_handler(url=f"/compat-405-{free_tcp_port}")
    @hg.compute_node
    def handler(request: hg.TS[compat.HttpRequest]) -> hg.TS[compat.HttpResponse]:
        called.append(request.value)
        return compat.HttpResponse(status_code=200)

    response = _call(
        free_tcp_port,
        f"/compat-405-{free_tcp_port}",
        method=web.HttpMethod.OPTIONS,
    )

    assert response.status == 405
    assert _header(response, "allow") == "GET, POST, PUT, DELETE"
    assert called == []


def test_translating_a_tornado_pattern_rejects_what_it_cannot_map():
    def patterns(url):
        return compat._translate_pattern(url).patterns

    assert patterns("/echo/(.*)") == ("/echo/*arg0",)
    assert patterns("^/echo/(.*)$") == ("/echo/*arg0",)
    assert patterns(r"/a/(\w+)/b/([^/]+)") == ("/a/{arg0}/b/{arg1}",)
    assert patterns("/plain") == ("/plain",)
    # The released REST route: an optional separator before the capture, which
    # one native pattern cannot express.
    assert patterns("/rest/?(.*)") == ("/rest/*arg0", "/rest")
    assert compat._translate_pattern("/rest/?(.*)").captures == 1

    with pytest.raises(ValueError, match="whole path segment"):
        compat._translate_pattern("/prefix-(.*)")
    with pytest.raises(ValueError, match="named or non-capturing"):
        compat._translate_pattern("/(?P<name>.*)")
    with pytest.raises(ValueError, match="unsupported regular-expression"):
        compat._translate_pattern("/items*")
    with pytest.raises(ValueError, match="separator before a trailing capture"):
        compat._translate_pattern("/items?/x")


def test_an_invalid_handler_signature_is_rejected_at_declaration():
    with pytest.raises(TypeError, match="requires a 'request'"):

        @compat.http_server_handler(url="/invalid/missing-request")
        def missing_request(value: hg.TS[int]) -> hg.TS[compat.HttpResponse]:
            return value

    with pytest.raises(TypeError, match="HTTP handler request must be"):

        @compat.http_server_handler(url="/invalid/request-type")
        def wrong_request(request: hg.TS[int]) -> hg.TS[compat.HttpResponse]:
            return request

    with pytest.raises(TypeError, match="HTTP handler output must be"):

        @compat.http_server_handler(url="/invalid/response-type")
        def wrong_response(request: hg.TS[compat.HttpRequest]) -> hg.TS[int]:
            return request


def test_wiring_a_handler_before_registering_the_server_is_an_error(free_tcp_port):
    @compat.http_server_handler(url=f"/compat-unregistered-{free_tcp_port}")
    @hg.compute_node
    def handler(request: hg.TS[compat.HttpRequest]) -> hg.TS[compat.HttpResponse]:
        return compat.HttpResponse(status_code=200)

    @hg.graph
    def app() -> None:
        handler()

    with pytest.raises(RuntimeError, match="register_http_server_adaptor"):
        hg.run_graph(app, end_time=hg.utc_now() + timedelta(seconds=1))


def test_a_websocket_handler_echoes_a_frame_per_connection(free_tcp_port):
    connected = []
    observed = []

    @compat.websocket_server_handler(url=f"/compat-ws-{free_tcp_port}")
    @hg.compute_node
    def handler(
        request: hg.TSB[compat.WebSocketServerRequest[str]],
    ) -> hg.TSB[compat.WebSocketResponse[str]]:
        if request["connect_request"].modified:
            connected.append(request["connect_request"].value)
            return {"connect_response": True}
        return {"message": f"echo:{request['messages'].value[0]}"}

    @hg.compute_node
    def client_key(
        stats: hg.TS[web.WebServerStats],
    ) -> hg.TS[web.WsClientKey]:
        if stats.value.listening_port == 0:
            return None
        return web.WsClientKey(
            f"ws://127.0.0.1:{free_tcp_port}/compat-ws-{free_tcp_port}"
        )

    @hg.compute_node
    def hello(event: hg.TS[web.WsEvent]) -> hg.TS[web.WsFrame]:
        if event.value.state != web.WsConnectionState.OPEN:
            return None
        return web.WsFrame.text_frame("ping")

    @hg.sink_node
    def capture(frame: hg.TS[web.WsFrame], _api: hg.EvaluationEngineApi = None):
        observed.append(frame.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        compat.register_websocket_server_adaptor(free_tcp_port, _server_config())
        web.register_web_client(_client_config(), path="api")
        key = client_key(
            web.web_server_stats(path=compat.compat_service_path(free_tcp_port))
        )
        client = web.web_ws_connect(key, path="api")
        web.web_ws_client_send(key, hello(client["event"]), path="api")
        capture(client["frame"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert len(connected) == 1
    assert connected[0].url == f"/compat-ws-{free_tcp_port}"
    assert [frame.text for frame in observed] == ["echo:ping"]


def test_a_rejected_websocket_connection_is_closed(free_tcp_port):
    """The native transport upgrades before the graph sees the connection.

    A ``connect_response`` of ``False`` therefore closes an accepted socket
    instead of refusing the handshake — the deviation recorded in compat's
    parity notes.
    """
    states = []

    @compat.websocket_server_handler(url=f"/compat-ws-deny-{free_tcp_port}")
    @hg.compute_node
    def handler(
        request: hg.TSB[compat.WebSocketServerRequest[str]],
    ) -> hg.TSB[compat.WebSocketResponse[str]]:
        if request["connect_request"].modified:
            return {"connect_response": False}
        return None

    @hg.compute_node
    def client_key(stats: hg.TS[web.WebServerStats]) -> hg.TS[web.WsClientKey]:
        if stats.value.listening_port == 0:
            return None
        return web.WsClientKey(
            f"ws://127.0.0.1:{free_tcp_port}/compat-ws-deny-{free_tcp_port}"
        )

    @hg.sink_node
    def capture(event: hg.TS[web.WsEvent], _api: hg.EvaluationEngineApi = None):
        states.append(event.value.state)
        if event.value.state != web.WsConnectionState.OPEN:
            _api.request_engine_stop()

    @hg.graph
    def app():
        compat.register_websocket_server_adaptor(free_tcp_port, _server_config())
        web.register_web_client(_client_config(), path="api")
        client = web.web_ws_connect(
            client_key(
                web.web_server_stats(path=compat.compat_service_path(free_tcp_port))
            ),
            path="api",
        )
        capture(client["event"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert states[0] == web.WsConnectionState.OPEN
    assert states[-1] != web.WsConnectionState.OPEN
