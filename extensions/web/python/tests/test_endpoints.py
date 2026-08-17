"""Tests for the RFC 0024 authoring decorators and codec helpers.

The behavioural tests run a real socket: the graph is both the server and the
client, the listening port is chosen by the OS (``port=0``) and discovered
from the server's own stats stream, and the engine stops as soon as the
expected traffic has been observed. The shape tests at the end need no socket
— they assert what the decorators accept and reject while wiring.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta

import pytest

import _hgraph
import hgraph as hg
from hgraph.test import use_wiring

try:
    import hgraph_web as web
except ImportError as error:  # pragma: no cover - exercised without the wheel
    pytest.skip(
        f"the hgraph-web native module is not importable: {error}",
        allow_module_level=True,
    )

import _loopback_harness as harness


# Annotations are resolved against this module's globals, so any type named in
# a handler signature has to live here rather than inside a test function.
@dataclass(frozen=True)
class Greeting(hg.CompoundScalar, namespace="hgraph.web.tests"):
    name: str = ""
    count: int = 0


class AuditedResponse(hg.TimeSeriesSchema, namespace="hgraph.web.tests"):
    response: hg.TS[web.HttpResponse]
    seen: hg.TS[int]


class AuditedSend(hg.TimeSeriesSchema, namespace="hgraph.web.tests"):
    connection_id: hg.TS[int]
    frame: hg.TS[web.WsFrame]
    seen: hg.TS[int]


def _content_type(response) -> str:
    return next(
        (
            header.value
            for header in response.headers
            if header.name.lower() == "content-type"
        ),
        "",
    )


def test_an_http_endpoint_round_trips_a_json_body() -> None:
    """``request_json`` and ``json_response`` compose the native JSON codec."""

    observed = []
    failures = []
    requested = []

    @hg.compute_node
    def bump(payload: hg.TS[Greeting]) -> hg.TS[Greeting]:
        greeting = payload.value
        return Greeting(name=greeting.name, count=greeting.count + 1)

    @web.http_endpoint(web.WebRoute(web.HttpMethod.POST, "/greet"), path="site")
    @hg.graph
    def greet(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        return web.json_response(bump(web.request_json(request, Greeting)))

    client_request = harness.port_triggered_request(
        requested,
        lambda port: web.HttpClientRequest(
            web.HttpMethod.POST,
            f"http://127.0.0.1:{port}/greet",
            headers=(web.WebHeader("content-type", "application/json"),),
            body=b'{"name": "py", "count": 2}',
        ),
    )
    capture, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        harness.register_loopback()

        greet()

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
    assert _content_type(observed[0]) == "application/json"
    assert json.loads(observed[0].body) == {"name": "py", "count": 3}


def test_an_authenticator_refuses_a_request_before_the_handler_sees_it() -> None:
    """A denied request is answered from the verdict; the handler never runs.

    The allowed request is issued only once the refusal has arrived, so the
    two requests cannot share an engine cycle and ``handled`` records exactly
    the requests that reached the handler.
    """

    handled = []
    observed = []
    failures = []
    port = []
    resent = []

    @hg.compute_node
    def verify(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.AuthResult]:
        headers = request.value.request.headers
        token = next(
            (header.value for header in headers if header.name.lower() == "x-token"),
            "",
        )
        if token == "open-sesame":
            return web.AuthResult.allow()
        return web.AuthResult.deny(401, "no token")

    @hg.graph
    def authenticate(
        request: hg.TS[web.HttpServerRequest],
    ) -> hg.TS[web.AuthResult]:
        return verify(request)

    @web.http_endpoint("/private", path="site", auth=authenticate)
    @hg.compute_node
    def private(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        handled.append(request.value.request.path)
        return web.HttpResponse(200, body=b"secret")

    @hg.compute_node
    def anonymous_request(
        stats: hg.TS[web.WebServerStats],
    ) -> hg.TS[web.HttpClientRequest]:
        listening = stats.value.listening_port
        if listening == 0 or port:
            return None
        port.append(listening)
        return web.HttpClientRequest(
            web.HttpMethod.GET, f"http://127.0.0.1:{listening}/private"
        )

    @hg.compute_node
    def authorised_request(
        refusal: hg.TS[web.HttpResponse],
    ) -> hg.TS[web.HttpClientRequest]:
        del refusal
        if resent:
            return None
        resent.append(True)
        return web.HttpClientRequest(
            web.HttpMethod.GET,
            f"http://127.0.0.1:{port[0]}/private",
            headers=(web.WebHeader("x-token", "open-sesame"),),
        )

    @hg.sink_node
    def capture_refusal(response: hg.TS[web.HttpResponse]):
        observed.append(response.value)

    capture_secret, capture_failure = harness.result_captures(observed, failures)

    @hg.graph
    def app():
        harness.register_loopback()

        private()

        refused = web.web_http_request(
            anonymous_request(web.web_server_stats(path="site")), path="api"
        )
        capture_refusal(refused["response"])
        capture_failure(refused["failure"])

        accepted = web.web_http_request(
            authorised_request(refused["response"]), path="api"
        )
        capture_secret(accepted["response"])
        capture_failure(accepted["failure"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert failures == []
    assert len(observed) == 2
    assert (observed[0].status, observed[0].body) == (401, b"no token")
    assert (observed[1].status, observed[1].body) == (200, b"secret")
    assert handled == ["/private"]


def test_a_ws_endpoint_echoes_a_text_frame_over_a_real_socket() -> None:
    """The WS decorator owns the serve/send wiring for an upgrade route."""

    received = []
    keys = []
    pinged = []

    @hg.compute_node
    def text_connection_id(frame: hg.TS[web.WsInboundFrame]) -> hg.TS[int]:
        # Kept in lockstep with ``ws_text`` so both arms of the send request
        # tick together for exactly the frames that are echoed.
        inbound = frame.value.frame
        if inbound is None or inbound.kind != web.WsFrameKind.TEXT:
            return None
        return frame.value.connection_id

    @web.ws_endpoint("/live", path="site")
    @hg.graph
    def live(
        event: hg.TS[web.WsEvent], frame: hg.TS[web.WsInboundFrame]
    ) -> hg.TSB[web.WsSendRequest]:
        del event
        return hg.TSB[web.WsSendRequest].from_ts(
            connection_id=text_connection_id(frame),
            frame=web.text_frame(web.ws_text(frame)),
        )

    @hg.compute_node
    def client_key(stats: hg.TS[web.WebServerStats]) -> hg.TS[web.WsClientKey]:
        port = stats.value.listening_port
        if port == 0 or keys:
            return None
        keys.append(port)
        return web.WsClientKey(f"ws://127.0.0.1:{port}/live")

    @hg.compute_node
    def ping(event: hg.TS[web.WsEvent]) -> hg.TS[web.WsFrame]:
        if pinged or event.value.state != web.WsConnectionState.OPEN:
            return None
        pinged.append(True)
        return web.WsFrame.text_frame("ping")

    @hg.sink_node
    def capture(
        frame: hg.TS[web.WsFrame],
        _api: hg.EvaluationEngineApi = None,
    ):
        received.append(frame.value)
        _api.request_engine_stop()

    @hg.graph
    def app():
        web.register_web_server(
            web.WebServerConfig(port=0, stats_interval_ms=50), path="site"
        )
        web.register_web_client(web.WebClientConfig(), path="api")

        live()

        key = client_key(web.web_server_stats(path="site"))
        connected = web.web_ws_connect(key, path="api")
        web.web_ws_client_send(key, ping(connected["event"]), path="api")
        capture(connected["frame"])

    hg.run_graph(
        app,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=hg.utc_now() + timedelta(seconds=20),
    )

    assert len(received) == 1
    assert received[0].kind == web.WsFrameKind.TEXT
    assert received[0].text == "ping"


def test_auxiliary_output_handlers_return_their_bundle_to_the_caller() -> None:
    """The response arm answers; the extra fields stay wirable (RFC 0024)."""

    @hg.compute_node
    def request_id(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request.value.request_id

    @hg.compute_node
    def accept(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        del request
        return web.HttpResponse(200)

    @hg.compute_node
    def connection_id(frame: hg.TS[web.WsInboundFrame]) -> hg.TS[int]:
        return frame.value.connection_id

    @web.http_endpoint("/orders", path="site")
    @hg.graph
    def orders(request: hg.TS[web.HttpServerRequest]) -> hg.TSB[AuditedResponse]:
        return hg.TSB[AuditedResponse].from_ts(
            response=accept(request), seen=request_id(request)
        )

    @web.ws_endpoint("/live", path="site")
    @hg.graph
    def live(frame: hg.TS[web.WsInboundFrame]) -> hg.TSB[AuditedSend]:
        return hg.TSB[AuditedSend].from_ts(
            connection_id=connection_id(frame),
            frame=web.text_frame(web.ws_text(frame)),
            seen=connection_id(frame),
        )

    wiring = _hgraph.Wiring()
    with use_wiring(wiring):
        web.register_web_server(web.WebServerConfig(port=0), path="site")

        served = orders()
        upgraded = live()
        hg.debug_print("seen", served["seen"])
        hg.debug_print("ws_seen", upgraded["seen"])

        # Calling an endpoint twice in one graph must reuse the wiring rather
        # than subscribe to the same route a second time.
        assert orders() is served
        assert live() is upgraded

    wiring.build_services()


def test_a_misdeclared_endpoint_is_rejected_while_it_is_wired() -> None:
    """Shape errors surface at decoration, not when a socket is bound."""

    @hg.compute_node
    def accept(request: hg.TS[web.HttpServerRequest]) -> hg.TS[web.HttpResponse]:
        del request
        return web.HttpResponse(200)

    @hg.graph
    def responder(
        request: hg.TS[web.HttpServerRequest],
    ) -> hg.TS[web.HttpResponse]:
        return accept(request)

    @hg.compute_node
    def request_id(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request.value.request_id

    @hg.graph
    def wrong_output(request: hg.TS[web.HttpServerRequest]) -> hg.TS[int]:
        return request_id(request)

    @hg.graph
    def wrong_input(other: hg.TS[int]) -> hg.TS[web.HttpResponse]:
        del other
        return hg.const(web.HttpResponse(200), tp=hg.TS[web.HttpResponse])

    upgrade_route = web.WebRoute(web.HttpMethod.GET, "/x", upgrade=True)
    plain_route = web.WebRoute(web.HttpMethod.GET, "/x")

    with pytest.raises(ValueError, match="upgrade=False"):
        web.http_endpoint(upgrade_route)(responder)
    with pytest.raises(ValueError, match="upgrade=True"):
        web.ws_endpoint(plain_route)(responder)
    with pytest.raises(TypeError, match="must return TS\\[HttpResponse\\]"):
        web.http_endpoint("/x")(wrong_output)
    with pytest.raises(TypeError, match="requires a 'request'"):
        web.http_endpoint("/x")(wrong_input)
    with pytest.raises(TypeError, match="must return TS\\[AuthResult\\]"):
        web.http_endpoint("/x", auth=wrong_output)(responder)
    with pytest.raises(TypeError, match="requires an 'event' or 'frame'"):
        web.ws_endpoint("/x")(responder)
