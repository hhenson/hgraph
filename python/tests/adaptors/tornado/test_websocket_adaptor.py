import asyncio
from datetime import datetime, timedelta, timezone
import socket
from threading import Thread
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from tornado.httpclient import HTTPClientError
from tornado.websocket import websocket_connect

import hgraph as hg
from hgraph.adaptors.tornado import (
    WebSocketClientRequest,
    WebSocketConnectRequest,
    WebSocketResponse,
    WebSocketServerRequest,
    register_websocket_server_adaptor,
    websocket_client_adaptor,
    websocket_client_adaptor_impl,
    websocket_server_handler,
    websocket_server_adaptor_helper,
    websocket_server_adaptor_impl,
)
from hgraph.adaptors.tornado.websocket_server_adaptor import (
    WebSocketAdaptorManager,
    WebSocketHandler,
)


@pytest.fixture
def free_tcp_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_feedback_supports_keyed_websocket_response_bundles():
    response_type = hg.TSD[int, hg.TSB[WebSocketResponse[bytes]]]

    @hg.graph
    def delayed(response: response_type) -> response_type:
        response_feedback = hg.feedback(response_type)
        response_feedback(response)
        return response_feedback()

    assert hg.eval_node(
        delayed,
        [{1: {"connect_response": True}}, None],
    ) == [None, {1: {"connect_response": True}}]


def test_websocket_manager_buffers_open_until_route_queues_start(free_tcp_port):
    path = f"/websocket-pending-{free_tcp_port}/(.*)"
    manager = WebSocketAdaptorManager(free_tcp_port, bytes)
    connect_events = []
    message_events = []

    async def exercise():
        request = WebSocketConnectRequest(url=path)
        request_id, accepted = manager.add_request(path, request, lambda message: None)
        assert connect_events == []
        assert not accepted.done()

        manager.set_queues(path, connect_events.append, message_events.append)
        assert connect_events == [{request_id: request}]
        manager.complete_request(request_id, {"connect_response": True})
        assert await accepted is True

        manager.remove_request(request_id)
        assert connect_events[-1] == {request_id: hg.REMOVE_IF_EXISTS}
        assert message_events == [{request_id: hg.REMOVE_IF_EXISTS}]

    asyncio.run(exercise())


def test_websocket_handler_open_propagates_cancellation_after_closing():
    async def exercise():
        response = asyncio.get_running_loop().create_future()
        response.cancel()
        manager = SimpleNamespace(
            binary=False,
            add_request=lambda _path, _request, _sender: (1, response),
        )
        handler = SimpleNamespace(
            _path="/cancelled",
            _manager=manager,
            _request_id=None,
            _accepted=False,
            request=SimpleNamespace(headers={}, cookies={}),
            write_message=Mock(),
            close=Mock(),
        )

        with pytest.raises(asyncio.CancelledError):
            await WebSocketHandler.open(handler)
        handler.close.assert_called_once_with()

    asyncio.run(exercise())


def test_websocket_managers_are_isolated_by_port_and_message_type(free_tcp_port):
    text_manager = WebSocketAdaptorManager.instance(free_tcp_port, str)
    binary_manager = WebSocketAdaptorManager.instance(free_tcp_port, bytes)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        other_port = sock.getsockname()[1]
    other_manager = WebSocketAdaptorManager.instance(other_port, str)

    assert text_manager is not binary_manager
    assert text_manager is not other_manager
    assert text_manager._web is binary_manager._web
    assert text_manager._web is not other_manager._web


def test_websocket_server_handler_rejects_invalid_authoring_signatures():
    with pytest.raises(TypeError, match="requires a 'request'"):

        @websocket_server_handler(url="/invalid/missing-request")
        def missing_request(value: hg.TS[int]) -> hg.TSB[WebSocketResponse[str]]:
            return value

    with pytest.raises(TypeError, match="WebSocket handler request must be"):

        @websocket_server_handler(url="/invalid/request-type")
        def wrong_request(request: hg.TS[int]) -> hg.TSB[WebSocketResponse[str]]:
            return request


def test_websocket_server_handler_round_trips_binary_messages(free_tcp_port):
    route = f"/websocket-{free_tcp_port}/(.*)"
    client_results = []
    client_errors = []
    threads = []

    @websocket_server_handler(url=route)
    def echo(
        request: hg.TSB[WebSocketServerRequest[bytes]],
        suffix: hg.TS[bytes],
    ) -> hg.TSB[WebSocketResponse[bytes]]:
        return hg.combine[hg.TSB[WebSocketResponse[bytes]]](
            connect_response=suffix == suffix,
            message=request.messages[-1],
        )

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        async def communicate():
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    socket = await websocket_connect(
                        f"ws://127.0.0.1:{free_tcp_port}/websocket-{free_tcp_port}/client",
                        connect_timeout=3.0,
                    )
                    break
                except (ConnectionRefusedError, OSError, HTTPClientError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.02)
            try:
                socket.write_message(b"one", binary=True)
                client_results.append(await socket.read_message())
                socket.write_message(b"two", binary=True)
                client_results.append(await socket.read_message())
            finally:
                socket.close()

        def run():
            try:
                asyncio.run(communicate())
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        thread = Thread(target=run, name="hgraph-websocket-test", daemon=True)
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
        hg.register_adaptor(
            None,
            websocket_server_adaptor_helper,
            port=free_tcp_port,
        )
        hg.register_adaptor(
            "websocket_server_adaptor",
            websocket_server_adaptor_impl,
            port=free_tcp_port,
        )
        echo(suffix=hg.const(b"-reply"))
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
    assert client_results == [b"one", b"two"]
    assert not any(key.startswith("websocket_server_adaptor://") for key in state.keys())


def test_websocket_server_handler_rejects_connection(free_tcp_port):
    route = f"/websocket-reject-{free_tcp_port}/(.*)"
    client_results = []
    client_errors = []

    @websocket_server_handler(url=route)
    @hg.compute_node
    def reject(
        request: hg.TSB[WebSocketServerRequest[str]],
    ) -> hg.TSB[WebSocketResponse[str]]:
        if request.connect_request.modified:
            return {"connect_response": False}

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        async def communicate():
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    connection = await websocket_connect(
                        f"ws://127.0.0.1:{free_tcp_port}/"
                        f"websocket-reject-{free_tcp_port}/client",
                        connect_timeout=3.0,
                    )
                    client_results.append(await connection.read_message())
                    return
                except (ConnectionRefusedError, OSError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.02)

        def run():
            try:
                asyncio.run(communicate())
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-websocket-reject-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_client()
        register_websocket_server_adaptor(free_tcp_port)
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
    assert client_results == [None]
    assert not any(key.startswith("websocket_server_adaptor://") for key in state.keys())


def test_websocket_server_handler_multiplexes_batch_requests(free_tcp_port):
    route = f"/websocket-batch-{free_tcp_port}/(.*)"
    connections = []
    client_results = []
    client_errors = []

    @websocket_server_handler(url=route)
    @hg.compute_node
    def echo(
        request: hg.TSD[int, hg.TSB[WebSocketServerRequest[bytes]]],
        _state: hg.STATE = None,
    ) -> hg.TSD[int, hg.TSB[WebSocketResponse[bytes]]]:
        _state.evaluations = getattr(_state, "evaluations", 0) + 1
        responses = {}
        for request_id, request_value in request.modified_items():
            response = {}
            if request_value.connect_request.modified:
                connections.append(request_value.connect_request.value)
                response["connect_response"] = True
            if request_value.messages.modified:
                response["message"] = request_value.messages.value[-1]
            if response:
                responses[request_id] = response
        return responses

    @hg.push_queue(hg.TS[bool])
    def drive_clients(sender):
        async def communicate(name):
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    socket = await websocket_connect(
                        f"ws://127.0.0.1:{free_tcp_port}/"
                        f"websocket-batch-{free_tcp_port}/{name}",
                        connect_timeout=3.0,
                    )
                    break
                except (ConnectionRefusedError, OSError, HTTPClientError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.02)
            try:
                message = name.encode()
                socket.write_message(message, binary=True)
                return await socket.read_message()
            finally:
                socket.close()

        def run():
            async def run_all():
                client_results.extend(
                    await asyncio.gather(
                        communicate("one"),
                        communicate("two"),
                    )
                )

            try:
                asyncio.run(run_all())
            except BaseException as error:
                client_errors.append(error)
            finally:
                sender(True)

        Thread(target=run, name="hgraph-websocket-batch-test", daemon=True).start()

    @hg.sink_node
    def stop_when_done(
        done: hg.TS[bool],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        _engine.request_engine_stop()

    @hg.graph
    def server_graph() -> None:
        done = drive_clients()
        hg.register_adaptor(
            "websocket_server_adaptor",
            websocket_server_adaptor_impl,
            port=free_tcp_port,
        )
        # ACE explicitly wires selected handlers after registering the legacy
        # server implementation. The second call must reuse the automatic
        # wiring rather than create a duplicate adaptor client.
        echo()
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
    assert sorted(client_results) == [b"one", b"two"]
    assert sorted(value.url_parsed_args[0] for value in connections) == ["one", "two"]
    assert not any(key.startswith("websocket_server_adaptor://") for key in state.keys())


@pytest.mark.parametrize(
    ("message_type", "payloads"),
    [
        (bytes, (b"one", b"two")),
        (str, ("one", "two")),
    ],
)
def test_websocket_client_service_adaptor_infers_message_specialization(
    free_tcp_port,
    message_type,
    payloads,
):
    route = f"/websocket-client-{message_type.__name__}-{free_tcp_port}/(.*)"
    received = []
    threads = []
    message_ts = hg.TS[message_type]
    server_request_type = hg.TSB[WebSocketServerRequest[message_type]]
    server_response_type = hg.TSB[WebSocketResponse[message_type]]
    client_request_type = hg.TSB[WebSocketClientRequest[message_type]]

    @websocket_server_handler(url=route)
    @hg.compute_node
    def echo(
        request: server_request_type,
    ) -> server_response_type:
        result = {}
        if request.connect_request.modified:
            result["connect_response"] = True
        if request.messages.modified:
            result["message"] = request.messages.value[-1]
        return result

    @hg.push_queue(message_ts)
    def messages(sender):
        def run():
            time.sleep(0.05)
            sender(payloads[0])
            time.sleep(0.02)
            sender(payloads[1])

        thread = Thread(target=run, name="hgraph-websocket-client-test", daemon=True)
        threads.append(thread)
        thread.start()

    @hg.sink_node
    def capture(
        message: message_ts,
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        received.append(message.value)
        if len(received) == 2:
            _engine.request_engine_stop()

    @hg.graph
    def application() -> None:
        register_websocket_server_adaptor(free_tcp_port)
        hg.register_adaptor(None, websocket_client_adaptor_impl)
        request = hg.combine[client_request_type](
            connect_request=hg.const(
                WebSocketConnectRequest(
                    f"ws://127.0.0.1:{free_tcp_port}/websocket-client-"
                    f"{message_type.__name__}-{free_tcp_port}/client"
                ),
                tp=hg.TS[WebSocketConnectRequest],
            ),
            message=messages(),
        )
        response = websocket_client_adaptor(
            request,
        )
        capture(response.message)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )
    for thread in threads:
        thread.join(timeout=5.0)

    assert received == list(payloads)
    assert not any(key.startswith("websocket_client_adaptor://") for key in state.keys())
