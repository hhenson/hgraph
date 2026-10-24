from datetime import datetime, timedelta, timezone
import http.client
import base64
import struct
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import random
import socket
from threading import Thread
import time
from urllib.parse import parse_qs, urlparse

import pytest

import hgraph as hg
from hgraph.adaptors.tornado import (
    Credentials,
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
    http_client_adaptor,
    http_client_adaptor_impl,
    http_server_adaptor,
    http_server_handler,
    http_server_adaptor_impl,
    register_http_server_adaptor,
)


@hg.compute_node
def _echo_request(request: hg.TS[HttpRequest]) -> hg.TS[HttpRequest]:
    return request.value


@hg.compute_node
def _echo_request_dict(
    requests: hg.TSD[int, hg.TS[HttpRequest]],
) -> hg.TSD[int, hg.TS[HttpRequest]]:
    return {
        request_id: request.value
        for request_id, request in requests.modified_items()
    }


@pytest.mark.parametrize(
    "request_value",
    [
        HttpGetRequest("http://example.test/read", query={"page": "2"}),
        HttpPostRequest("http://example.test/create", body="created"),
        HttpPutRequest("http://example.test/update", body="updated"),
        HttpDeleteRequest("http://example.test/delete"),
    ],
)
def test_http_request_closed_union_round_trips_through_eval_node(request_value):
    assert hg.eval_node(_echo_request, [request_value]) == [request_value]


@pytest.mark.parametrize("elide", [False, True])
def test_http_request_closed_union_records_different_leaves(elide):
    requests = [
        HttpGetRequest("http://example.test/read"),
        HttpPostRequest("http://example.test/create", body="created"),
    ]
    assert hg.eval_node(_echo_request, requests, __elide__=elide) == requests


@pytest.mark.parametrize("elide", [False, True])
def test_http_request_closed_union_tsd_round_trips_through_eval_node(elide):
    request = HttpPostRequest("http://example.test/create", body="created")

    assert hg.eval_node(_echo_request_dict, [{1: request}], __elide__=elide) == [
        {1: request}
    ]


def test_http_client_credentials_and_timeouts_lower_to_native_options():
    from hgraph_web.compat_client import _http_call

    request = HttpGetRequest(
        "http://localhost/resource?existing=1", query={"item": "a b"},
        auth=Credentials("user", "password"),
        connect_timeout=1.5, request_timeout=2.25,
    )
    [result] = hg.eval_node(_http_call, [request])
    assert result["request"].url == "http://localhost/resource?existing=1&item=a+b"
    options = result["options"]
    assert options.integrated_auth is True
    assert options.username == "user"
    assert options.password == "password"
    assert options.connect_timeout_ms == 1500
    assert options.request_timeout_ms == 2250
    assert "password" not in repr(options)
    assert str(request.auth) == "credentials"


@pytest.mark.parametrize("auth", [None, Credentials("", "")])
def test_http_client_platform_credentials_require_explicit_opt_in(auth):
    from hgraph_web.compat_client import _http_call

    [result] = hg.eval_node(_http_call, [HttpGetRequest("http://localhost/", auth=auth)])
    assert result["options"].integrated_auth is (auth is not None)


@pytest.mark.parametrize("timeout, expected_ms", [(0, 0), (0.0005, 1), (0.0011, 2)])
def test_http_client_positive_timeouts_round_up(timeout, expected_ms):
    from hgraph_web.compat_client import _http_call

    [result] = hg.eval_node(_http_call, [HttpGetRequest(
        "http://localhost/", connect_timeout=timeout, request_timeout=timeout)])
    assert result["options"].connect_timeout_ms == expected_ms
    assert result["options"].request_timeout_ms == expected_ms


@pytest.fixture
def http_endpoint():
    received = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode() if length else ""
            parsed = urlparse(self.path)
            if parsed.path == "/ntlm":
                token = self.headers.get("Authorization", "")
                received.append({"authorization": token})
                if not token:
                    challenge = "NTLM"
                else:
                    message = base64.b64decode(token.removeprefix("NTLM "))
                    if message[8:12] == b"\x03\0\0\0":
                        length, _, offset = struct.unpack_from("<HHI", message, 36)
                        username = message[offset:offset + length].decode("utf-16-le")
                        response = username.encode()
                        self.send_response(200)
                        self.send_header("Content-Length", str(len(response)))
                        self.end_headers()
                        self.wfile.write(response)
                        return
                    challenge = "NTLM TlRMTVNTUAACAAAAAAAAADAAAAAFgooAMTIzNDU2NzgAAAAAAAAAAAQABAAwAAAAAAAAAA=="
                self.send_response(401)
                self.send_header("WWW-Authenticate", challenge)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if parsed.path == "/slow":
                time.sleep(0.2)
            received.append(
                {
                    "method": self.command,
                    "path": parsed.path,
                    "query": parse_qs(parsed.query),
                    "body": body,
                    "header": self.headers.get("X-HGraph-Test"),
                }
            )
            status = 418 if parsed.path == "/status" else 201 if self.command == "POST" else 200
            response = f"{self.command}:{parsed.path}:{body}".encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain")
            self.send_header("X-HGraph-Method", self.command)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

        do_GET = _handle
        do_POST = _handle
        do_PUT = _handle
        do_DELETE = _handle

        def log_message(self, _format, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, name="hgraph-http-test", daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", received
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5.0)


@pytest.fixture
def free_tcp_port():
    # Allocate OUTSIDE the OS ephemeral range: a probe-close-bind port from
    # the ephemeral range can be reissued as an outbound SOURCE port (client
    # connection pools) before the server binds it — the "Address already in
    # use" flake seen on the macOS CI leg.
    start = random.randint(20000, 31000)
    for offset in range(1000):
        candidate = start + offset
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", candidate))
            except OSError:
                continue
            return candidate
    raise RuntimeError("no free TCP port found")


@pytest.mark.parametrize(
    ("method", "request_factory", "expected_status", "expected_body"),
    [
        (
            "GET",
            lambda url: HttpGetRequest(
                f"{url}/read",
                query={"item": "42"},
                headers={"X-HGraph-Test": "get"},
            ),
            200,
            b"GET:/read:",
        ),
        (
            "POST",
            lambda url: HttpPostRequest(
                f"{url}/create",
                headers={"X-HGraph-Test": "post"},
                body="created",
            ),
            201,
            b"POST:/create:created",
        ),
        (
            "PUT",
            lambda url: HttpPutRequest(
                f"{url}/update",
                headers={"X-HGraph-Test": "put"},
                body="updated",
            ),
            200,
            b"PUT:/update:updated",
        ),
        (
            "DELETE",
            lambda url: HttpDeleteRequest(
                f"{url}/delete",
                headers={"X-HGraph-Test": "delete"},
            ),
            200,
            b"DELETE:/delete:",
        ),
    ],
)
def test_http_client_service_adaptor(
    http_endpoint,
    method,
    request_factory,
    expected_status,
    expected_body,
):
    base_url, received = http_endpoint
    request = request_factory(base_url)
    path = f"http-client-test-{method.lower()}"
    responses = []

    @hg.push_queue(hg.TS[HttpRequest])
    def request_source(sender):
        sender(request)

    @hg.sink_node
    def capture_response(
        response: hg.TS[HttpResponse],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        responses.append(response.value)
        _engine.request_engine_stop()

    @hg.graph
    def client_graph() -> None:
        hg.register_adaptor(path, http_client_adaptor_impl)
        response = http_client_adaptor(
            request_source(),
            path=path,
        )
        capture_response(response)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            client_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=3),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert len(responses) == 1
    response = responses[0]
    assert response.status_code == expected_status
    assert response.headers["X-Hgraph-Method"] == method
    assert response.body == expected_body
    assert received == [
        {
            "method": method,
            "path": urlparse(request.url).path,
            "query": {"item": ["42"]} if method == "GET" else {},
            "body": request.body if method in {"POST", "PUT"} else "",
            "header": method.lower(),
        }
    ]
    assert f"http_client_adaptor://{path}/queue" not in state


def test_http_client_concurrent_requests_match_upstream_recorded_flow(free_tcp_port):
    """Near-verbatim port of upstream's real-time HTTP client graph test."""
    route = f"/recorded-client-{free_tcp_port}/(.*)"

    @http_server_handler(url=route)
    def echo(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
        return hg.combine[hg.TS[HttpResponse]](
            status_code=200,
            body=hg.convert[hg.TS[bytes]](request.url_parsed_args[0]),
        )

    @hg.sink_node
    def stop_when_all_done(
        values: hg.TSD[str, hg.TS[bool]],
        expected_count: int,
        _engine: hg.EvaluationEngineApi = None,
        _state: hg.STATE = None,
    ) -> None:
        _state.done_count = getattr(_state, "done_count", 0)
        _state.done_count += sum(bool(value) for value in values.delta_value.values())
        if _state.done_count >= expected_count:
            _engine.request_engine_stop()

    @hg.graph
    def application() -> None:
        register_http_server_adaptor(free_tcp_port)
        hg.register_adaptor("http_client", http_client_adaptor_impl)
        queries = {
            str(index): HttpGetRequest(
                f"http://127.0.0.1:{free_tcp_port}/recorded-client-"
                f"{free_tcp_port}/{index}"
            )
            for index in range(10)
        }

        @hg.graph
        def send_query(key: hg.TS[str], query: hg.TS[HttpRequest]) -> hg.TS[bool]:
            response = http_client_adaptor(query)
            return key == hg.convert[hg.TS[str]](response.body)

        result = hg.map_(
            send_query,
            query=hg.const(
                queries,
                tp=hg.TSD[str, hg.TS[HttpRequest]],
                delay=timedelta(milliseconds=2),
            ),
        )
        hg.record(result)
        stop_when_all_done(result, 10)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.evaluate_graph(
            application,
            hg.GraphConfiguration(
                run_mode=hg.EvaluationMode.REAL_TIME,
                end_time=timedelta(seconds=3),
            ),
        )
        recorded = hg.get_recorded_value()

    accumulated = {}
    for _timestamp, delta in recorded:
        accumulated.update(delta)
    assert accumulated == {str(index): True for index in range(10)}
    assert not any(key.startswith("http_client_adaptor://") for key in state.keys())
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())


def test_http_server_adaptor_round_trips_all_methods(free_tcp_port):
    route = "/items/(.*)"
    received = []
    responses = []
    client_errors = []
    threads = []

    @hg.compute_node
    def handle_requests(
        requests: hg.TSD[int, hg.TS[HttpRequest]],
    ) -> hg.TSD[int, hg.TS[HttpResponse]]:
        result = {}
        for request_id, request in requests.modified_items():
            value = request.value
            received.append(value)
            result[request_id] = HttpResponse(
                status_code=202,
                headers={"X-HGraph-Request": type(value).__name__},
                cookies={"session": {"value": "accepted", "path": "/"}},
                body=f"{value.url_parsed_args[0]}:{getattr(value, 'body', '')}".encode(),
            )
        return result

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def request(method, body=None):
            deadline = time.monotonic() + 10.0
            while True:
                connection = http.client.HTTPConnection(
                    "127.0.0.1",
                    free_tcp_port,
                    timeout=2.0,
                )
                try:
                    connection.request(
                        method,
                        "/items/widget?item=42",
                        body=body,
                        headers={
                            "Content-Type": "text/plain",
                            "Cookie": "client=web-test",
                            "X-HGraph-Test": method.lower(),
                        },
                    )
                    response = connection.getresponse()
                    if response.status == 404 and time.monotonic() < deadline:
                        time.sleep(0.02)
                        continue
                    responses.append(
                        {
                            "status": response.status,
                            "request_type": response.getheader("X-HGraph-Request"),
                            "cookie": response.getheader("Set-Cookie"),
                            "body": response.read(),
                        }
                    )
                    return
                except (ConnectionRefusedError, OSError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.02)
                finally:
                    connection.close()

        def run():
            try:
                request("GET")
                request("POST", "created")
                request("PUT", "updated")
                request("DELETE")
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        thread = Thread(target=run, name="hgraph-http-server-test", daemon=True)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        hg.register_adaptor(route, http_server_adaptor_impl, port=free_tcp_port)
        response_feedback = hg.feedback(hg.TSD[int, hg.TS[HttpResponse]])
        requests = http_server_adaptor(response_feedback(), path=route)
        response_feedback(handle_requests(requests))
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )
    for thread in threads:
        thread.join(timeout=5.0)

    assert client_errors == []
    assert [type(request) for request in received] == [
        HttpGetRequest,
        HttpPostRequest,
        HttpPutRequest,
        HttpDeleteRequest,
    ]
    assert all(request.url == route for request in received)
    assert all(request.url_parsed_args == ("widget",) for request in received)
    assert all(request.query == {"item": "42"} for request in received)
    assert all(request.cookies["client"]["value"] == "web-test" for request in received)
    assert [getattr(request, "body", "") for request in received] == [
        "",
        "created",
        "updated",
        "",
    ]
    assert [response["status"] for response in responses] == [202] * 4
    assert [response["request_type"] for response in responses] == [
        "HttpGetRequest",
        "HttpPostRequest",
        "HttpPutRequest",
        "HttpDeleteRequest",
    ]
    assert [response["body"] for response in responses] == [
        b"widget:",
        b"widget:created",
        b"widget:updated",
        b"widget:",
    ]
    assert all("session=accepted" in response["cookie"] for response in responses)
    assert f"http_server_adaptor://{free_tcp_port}/{route}/queue" not in state


def test_http_server_handler_registers_and_maps_single_requests(free_tcp_port):
    route = f"/handler-{free_tcp_port}/(.*)"
    received = []
    client_result = []
    client_errors = []

    @http_server_handler(url=route)
    @hg.compute_node
    def handler(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
        received.append(request.value)
        return HttpResponse(
            status_code=200,
            body=f"handled:{request.value.url_parsed_args[0]}".encode(),
        )

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1",
                        free_tcp_port,
                        timeout=2.0,
                    )
                    try:
                        connection.request("GET", f"/handler-{free_tcp_port}/item")
                        response = connection.getresponse()
                        if response.status == 404 and time.monotonic() < deadline:
                            time.sleep(0.02)
                            continue
                        client_result.append((response.status, response.read()))
                        break
                    except (ConnectionRefusedError, OSError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.02)
                    finally:
                        connection.close()
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-http-handler-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        register_http_server_adaptor(free_tcp_port)
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_result == [(200, b"handled:item")]
    assert len(received) == 1
    assert isinstance(received[0], HttpGetRequest)
    assert f"http_server_adaptor://{free_tcp_port}/{route}/queue" not in state


def test_http_server_supports_late_manual_handler_and_idempotent_call(free_tcp_port):
    route = f"/late-manual-{free_tcp_port}"
    observed = []

    @hg.sink_node
    def capture_and_stop(value: hg.TS[bool], _engine: hg.EvaluationEngineApi = None):
        observed.append(value.value)
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        register_http_server_adaptor(free_tcp_port)

        @http_server_handler(url=route)
        def handler(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
            return hg.combine[hg.TS[HttpResponse]](status_code=200)

        first = handler()
        second = handler()
        capture_and_stop(hg.const(first is second))

    hg.run_graph(
        server_graph,
        run_mode=hg.EvaluationMode.REAL_TIME,
        end_time=datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=5),
    )
    assert observed == [True]


def test_http_route_precedence_follows_handler_registration_order(free_tcp_port):
    prefix = f"/precedence-{free_tcp_port}"
    specific_route = f"{prefix}/specific/(.*)"
    broad_route = f"{prefix}/(.*)"
    result = []
    errors = []

    @http_server_handler(url=specific_route)
    @hg.compute_node
    def specific(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
        return HttpResponse(status_code=200, body=b"specific")

    @http_server_handler(url=broad_route)
    @hg.compute_node
    def broad(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
        return HttpResponse(status_code=200, body=b"broad")

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", free_tcp_port, timeout=2.0)
                    try:
                        connection.request("GET", f"{prefix}/specific/item")
                        response = connection.getresponse()
                        if response.status == 404 and time.monotonic() < deadline:
                            time.sleep(0.02)
                            continue
                        result.append(response.read())
                        break
                    except (ConnectionRefusedError, OSError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.02)
                    finally:
                        connection.close()
            except BaseException as error:
                errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-http-precedence-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def application() -> None:
        done = drive_client()
        register_http_server_adaptor(free_tcp_port)
        stop_when_done(done)

    with hg.GlobalContext(hg.GlobalState()):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert errors == []
    assert result == [b"specific"]


def test_http_server_handler_rejects_invalid_authoring_signatures():
    with pytest.raises(TypeError, match="requires a 'request'"):

        @http_server_handler(url="/invalid/missing-request")
        def missing_request(value: hg.TS[int]) -> hg.TS[HttpResponse]:
            return value

    with pytest.raises(TypeError, match="HTTP handler request must be"):

        @http_server_handler(url="/invalid/request-type")
        def wrong_request(request: hg.TS[int]) -> hg.TS[HttpResponse]:
            return request


def test_http_server_handler_registers_batch_requests(free_tcp_port):
    route = f"/batch-handler-{free_tcp_port}"
    received = []
    client_results = []
    client_errors = []

    @http_server_handler(url=route)
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[HttpRequest]],
    ) -> hg.TSD[int, hg.TS[HttpResponse]]:
        responses = {}
        for request_id, request_value in request.modified_items():
            received.append(request_value.value)
            responses[request_id] = HttpResponse(
                status_code=200,
                body=f"handled:{len(received)}".encode(),
            )
        return responses

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                for _ in range(2):
                    while True:
                        connection = http.client.HTTPConnection(
                            "127.0.0.1",
                            free_tcp_port,
                            timeout=2.0,
                        )
                        try:
                            connection.request("GET", route)
                            response = connection.getresponse()
                            if response.status == 404 and time.monotonic() < deadline:
                                time.sleep(0.02)
                                continue
                            client_results.append((response.status, response.read()))
                            break
                        except (ConnectionRefusedError, OSError):
                            if time.monotonic() >= deadline:
                                raise
                            time.sleep(0.02)
                        finally:
                            connection.close()
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-http-batch-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        register_http_server_adaptor(free_tcp_port)
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_results == [(200, b"handled:1"), (200, b"handled:2")]
    assert len(received) == 2
    assert all(isinstance(value, HttpGetRequest) for value in received)
    assert f"http_server_adaptor://{free_tcp_port}/{route}/queue" not in state


def test_http_server_handler_preserves_keyed_auxiliary_outputs(free_tcp_port):
    route = f"/batch-aux-handler-{free_tcp_port}"
    client_results = []
    client_errors = []
    captured_audits = []

    class HandlerOutput(hg.TimeSeriesSchema):
        response: hg.TS[HttpResponse]
        audit: hg.TS[str]

    output_type = hg.TSD[int, hg.TSB[HandlerOutput]]

    @http_server_handler(url=route)
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[HttpRequest]],
    ) -> output_type:
        return {
            request_id: {
                "response": HttpResponse(
                    status_code=202,
                    body=f"handled:{request_value.value.url}".encode(),
                ),
                "audit": type(request_value.value).__name__,
            }
            for request_id, request_value in request.modified_items()
        }

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1",
                        free_tcp_port,
                        timeout=2.0,
                    )
                    try:
                        connection.request("GET", route)
                        response = connection.getresponse()
                        if response.status == 404 and time.monotonic() < deadline:
                            time.sleep(0.02)
                            continue
                        client_results.append((response.status, response.read()))
                        break
                    except (ConnectionRefusedError, OSError):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.02)
                    finally:
                        connection.close()
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-http-aux-handler-test", daemon=True).start()

    @hg.sink_node
    def capture(audit: hg.TSD[int, hg.TS[str]]) -> None:
        captured_audits.extend(
            value.value for _, value in audit.modified_items()
        )

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        register_http_server_adaptor(free_tcp_port)
        outputs = handler()
        capture(outputs.audit)
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            server_graph,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_results == [(202, f"handled:{route}".encode())]
    assert captured_audits == ["HttpGetRequest"]
    assert f"http_server_adaptor://{free_tcp_port}/{route}/queue" not in state


def _eval_client_request(request, *, use_curl=False):
    @hg.compute_node
    def stop(response: hg.TS[HttpResponse], _engine: hg.EvaluationEngineApi = None) -> hg.TS[HttpResponse]:
        _engine.request_engine_stop()
        return response.value

    @hg.graph
    def client(request: hg.TS[HttpRequest]) -> hg.TS[HttpResponse]:
        hg.register_adaptor("native-client", http_client_adaptor_impl, use_curl=use_curl)
        return stop(http_client_adaptor(request, path="native-client"))

    start = datetime.now(timezone.utc).replace(tzinfo=None)
    return hg.eval_node(
        client, [request], __run_mode__=hg.EvaluationMode.REAL_TIME,
        __start_time__=start, __end_time__=start + timedelta(seconds=3),
        __elide__=True,
    )


@pytest.mark.parametrize("use_curl", [False, True])
def test_legacy_client_backend_flag_uses_native_transport(http_endpoint, use_curl):
    url, _ = http_endpoint
    [response] = _eval_client_request(HttpGetRequest(f"{url}/read"), use_curl=use_curl)
    assert response.status_code == 200
    assert response.body == b"GET:/read:"


def test_legacy_client_reports_connection_failure_and_invalid_request(free_tcp_port):
    [failed] = _eval_client_request(HttpGetRequest(f"http://127.0.0.1:{free_tcp_port}/missing"))
    assert failed.status_code == 400
    assert failed.body
    [invalid] = _eval_client_request(HttpGetRequest(""))
    assert invalid.status_code == 400
    assert invalid.body


def test_legacy_client_authenticates_with_native_ntlm(http_endpoint):
    url, _ = http_endpoint
    [response] = _eval_client_request(HttpGetRequest(
        f"{url}/ntlm", auth=Credentials("native-user", "password")))
    assert response.status_code == 200
    assert response.body == b"native-user"


def test_legacy_client_without_auth_does_not_answer_ntlm_challenge(http_endpoint):
    url, received = http_endpoint
    [response] = _eval_client_request(HttpGetRequest(f"{url}/ntlm", auth=None))
    assert response.status_code == 401
    assert received == [{"authorization": ""}]


class CustomGetRequest(HttpGetRequest):
    pass


class CustomPostRequest(HttpPostRequest):
    pass


class CustomPutRequest(HttpPutRequest):
    pass


class CustomDeleteRequest(HttpDeleteRequest):
    pass


@pytest.mark.parametrize("request_type, method, status", [
    (CustomGetRequest, "GET", 200),
    (CustomPostRequest, "POST", 201),
    (CustomPutRequest, "PUT", 200),
    (CustomDeleteRequest, "DELETE", 200),
])
def test_legacy_client_accepts_request_subclasses(http_endpoint, request_type, method, status):
    url, received = http_endpoint
    [response] = _eval_client_request(request_type(f"{url}/subclass"))
    assert response.status_code == status
    assert received[0]["method"] == method


def test_legacy_client_submillisecond_request_timeout_is_not_disabled(http_endpoint):
    url, _ = http_endpoint
    [response] = _eval_client_request(HttpGetRequest(f"{url}/slow", request_timeout=0.0005))
    assert response.status_code == 400
    assert response.body


def test_legacy_client_preserves_http_error_and_timeout_response_shapes(http_endpoint):
    url, _ = http_endpoint
    [response] = _eval_client_request(HttpGetRequest(f"{url}/status"))
    assert response.status_code == 418
    assert response.body == b"GET:/status:"
    [timeout] = _eval_client_request(HttpGetRequest(f"{url}/slow", request_timeout=0.05))
    assert timeout.status_code == 400
    assert timeout.body


def test_legacy_http_client_handles_successive_requests_on_one_caller(http_endpoint):
    url, _ = http_endpoint
    observed = []
    sender_ref = {}

    @hg.push_queue(hg.TS[HttpRequest])
    def requests(sender):
        sender_ref["sender"] = sender
        sender(HttpGetRequest(f"{url}/first"))

    @hg.sink_node
    def capture(response: hg.TS[HttpResponse], _engine: hg.EvaluationEngineApi = None) -> None:
        observed.append(response.value)
        if len(observed) == 1:
            sender_ref["sender"](HttpGetRequest(f"{url}/second"))
        else:
            _engine.request_engine_stop()

    @hg.graph
    def application() -> hg.TS[HttpResponse]:
        hg.register_adaptor("successive", http_client_adaptor_impl)
        response = http_client_adaptor(requests(), path="successive")
        capture(response)
        return response

    start = datetime.now(timezone.utc).replace(tzinfo=None)
    hg.eval_node(application, __run_mode__=hg.EvaluationMode.REAL_TIME,
                 __start_time__=start, __end_time__=start + timedelta(seconds=3), __elide__=True)
    assert [response.body for response in observed] == [b"GET:/first:", b"GET:/second:"]
