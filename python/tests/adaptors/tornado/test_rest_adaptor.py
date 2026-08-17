from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import http.client
import socket
from threading import Thread
import time

from frozendict import frozendict
import pytest

import hgraph as hg
from hgraph.adaptors.tornado import (
    HttpDeleteRequest,
    HttpGetRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpRequest,
    HttpResponse,
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
    register_http_server_adaptor,
    register_rest_client,
    rest_handler,
    rest_create,
    rest_delete,
    rest_list,
    rest_read,
    rest_update,
)


URL = "http://localhost/test"


@dataclass(frozen=True)
class MyCS(hg.CompoundScalar):
    a: int
    b: str


# Generic closed-union leaves must be declared before a graph realization is
# frozen. This is the documented closed-union rule, not adaptor test setup.
for _response_type in (
    RestReadResponse[MyCS],
    RestCreateResponse[MyCS],
    RestUpdateResponse[MyCS],
):
    hg.TS[_response_type]


@hg.graph
def _convert_to_request(ts: hg.TS[HttpRequest]) -> hg.TS[RestRequest]:
    return hg.convert[hg.TS[RestRequest]](ts, value_type=MyCS)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (HttpGetRequest(url=URL), RestListRequest(url=URL)),
        (
            HttpGetRequest(url=URL, url_parsed_args=("id1",)),
            RestReadRequest(url=URL, id="id1"),
        ),
        (
            HttpPostRequest(
                url=URL,
                body='{ "id": "id1", "value": { "a": 1, "b": "b" } }',
            ),
            RestCreateRequest[MyCS](
                url=URL,
                id="id1",
                value=MyCS(a=1, b="b"),
            ),
        ),
        (
            HttpPutRequest(
                url=URL,
                url_parsed_args=("id1",),
                body='{ "a": 1, "b": "b" }',
            ),
            RestUpdateRequest[MyCS](
                url=URL,
                id="id1",
                value=MyCS(a=1, b="b"),
            ),
        ),
        (
            HttpDeleteRequest(url=URL, url_parsed_args=("id1",)),
            RestDeleteRequest(url=URL, id="id1"),
        ),
    ],
)
def test_http_request_converts_to_rest_request(value, expected):
    assert hg.eval_node(_convert_to_request, [value]) == [expected]


@hg.graph
def _convert_from_rest_response(
    ts: hg.TS[RestResponse],
) -> hg.TS[HttpResponse]:
    return hg.convert[hg.TS[HttpResponse]](ts)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            RestListResponse(status=RestResultEnum.OK, ids=("1", "2")),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'[ "1", "2" ]',
            ),
        ),
        (
            RestReadResponse[MyCS](
                status=RestResultEnum.OK,
                id="1",
                value=MyCS(a=1, b="b"),
            ),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ),
        (
            RestCreateResponse[MyCS](
                status=RestResultEnum.CREATED,
                id="1",
                value=MyCS(a=1, b="b"),
            ),
            HttpResponse(
                status_code=201,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ),
        (
            RestUpdateResponse[MyCS](
                status=RestResultEnum.OK,
                id="1",
                value=MyCS(a=1, b="b"),
            ),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ),
        (
            RestDeleteResponse(status=RestResultEnum.OK),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b"",
            ),
        ),
        (
            RestDeleteResponse(
                status=RestResultEnum.NOT_FOUND,
                reason="Id not found",
            ),
            HttpResponse(
                status_code=404,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "reason": "Id not found" }',
            ),
        ),
    ],
)
def test_rest_response_converts_to_http_response(value, expected):
    assert hg.eval_node(_convert_from_rest_response, [value]) == [expected]


@hg.graph
def _convert_to_read_response(
    ts: hg.TS[HttpResponse],
) -> hg.TS[RestReadResponse[MyCS]]:
    return hg.convert[hg.TS[RestReadResponse[MyCS]]](ts)


@hg.graph
def _convert_to_create_response(
    ts: hg.TS[HttpResponse],
) -> hg.TS[RestCreateResponse[MyCS]]:
    return hg.convert[hg.TS[RestCreateResponse[MyCS]]](ts)


@hg.graph
def _convert_to_update_response(
    ts: hg.TS[HttpResponse],
) -> hg.TS[RestUpdateResponse[MyCS]]:
    return hg.convert[hg.TS[RestUpdateResponse[MyCS]]](ts)


@pytest.mark.parametrize(
    ("converter", "status_code", "expected_type", "expected_status"),
    [
        (_convert_to_read_response, 200, RestReadResponse, RestResultEnum.OK),
        (
            _convert_to_create_response,
            201,
            RestCreateResponse,
            RestResultEnum.CREATED,
        ),
        (_convert_to_update_response, 200, RestUpdateResponse, RestResultEnum.OK),
    ],
)
def test_http_response_converts_to_typed_rest_response(
    converter,
    status_code,
    expected_type,
    expected_status,
):
    result = hg.eval_node(
        converter,
        [
            HttpResponse(
                status_code=status_code,
                body=b'{"id":"1","value":{"a":1,"b":"b"}}',
            )
        ],
    )
    assert result == [
        expected_type[MyCS](
            status=expected_status,
            id="1",
            value=MyCS(a=1, b="b"),
        )
    ]


@pytest.fixture
def free_tcp_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_rest_handler_maps_live_delete_request(free_tcp_port):
    route = f"/rest-{free_tcp_port}"
    client_responses = []
    client_errors = []
    threads = []

    @rest_handler(url=route, data_type=MyCS)
    @hg.compute_node
    def handler(request: hg.TS[RestRequest]) -> hg.TS[RestResponse]:
        assert request.value == RestDeleteRequest(url=f"{route}/?(.*)", id="abc")
        return RestDeleteResponse(
            status=RestResultEnum.NOT_FOUND,
            reason="Hello, world!",
        )

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            deadline = time.monotonic() + 10.0
            try:
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", free_tcp_port, timeout=2.0
                    )
                    try:
                        connection.request("DELETE", f"{route}/abc")
                        response = connection.getresponse()
                        client_responses.append((response.status, response.read()))
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

        thread = Thread(target=run, name="hgraph-rest-handler-test", daemon=True)
        threads.append(thread)
        thread.start()

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

    assert client_errors == []
    assert client_responses == [(404, b'{ "reason": "Hello, world!" }')]
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())


def test_rest_handler_maps_batch_requests(free_tcp_port):
    route = f"/rest-batch-{free_tcp_port}"
    client_responses = []
    client_errors = []

    @rest_handler(url=route, data_type=MyCS)
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[RestRequest]],
        _state: hg.STATE = None,
    ) -> hg.TSD[int, hg.TS[RestResponse]]:
        responses = {}
        for request_id, request_value in request.modified_items():
            assert isinstance(request_value.value, RestDeleteRequest)
            _state.count = getattr(_state, "count", 0) + 1
            responses[request_id] = RestDeleteResponse(
                status=RestResultEnum.NOT_FOUND,
                reason=f"missing:{request_value.value.id}:{_state.count}",
            )
        return responses

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                for identifier in ("one", "two"):
                    while True:
                        connection = http.client.HTTPConnection(
                            "127.0.0.1",
                            free_tcp_port,
                            timeout=2.0,
                        )
                        try:
                            connection.request("DELETE", f"{route}/{identifier}")
                            response = connection.getresponse()
                            client_responses.append((response.status, response.read()))
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

        Thread(target=run, name="hgraph-rest-batch-test", daemon=True).start()

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

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_responses == [
        (404, b'{ "reason": "missing:one:1" }'),
        (404, b'{ "reason": "missing:two:2" }'),
    ]
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())


def test_rest_handler_preserves_auxiliary_outputs(free_tcp_port):
    route = f"/rest-aux-{free_tcp_port}"
    client_responses = []
    client_errors = []
    captured_audits = []

    class HandlerOutput(hg.TimeSeriesSchema):
        response: hg.TS[RestResponse]
        audit: hg.TS[str]

    @rest_handler(url=route, data_type=MyCS)
    @hg.compute_node
    def handler(
        request: hg.TS[RestRequest],
    ) -> hg.TSB[HandlerOutput]:
        value = request.value
        return {
            "response": RestDeleteResponse(
                status=RestResultEnum.NOT_FOUND,
                reason=f"missing:{value.id}",
            ),
            "audit": f"{type(value).__name__}:{value.id}",
        }

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", free_tcp_port, timeout=2.0
                    )
                    try:
                        connection.request("DELETE", f"{route}/abc")
                        response = connection.getresponse()
                        client_responses.append((response.status, response.read()))
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

        Thread(target=run, name="hgraph-rest-aux-handler-test", daemon=True).start()

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
    def application() -> None:
        done = drive_client()
        register_http_server_adaptor(free_tcp_port)
        outputs = handler()
        capture(outputs.audit)
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_responses == [(404, b'{ "reason": "missing:abc" }')]
    assert captured_audits == ["RestDeleteRequest:abc"]
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())


def test_batch_rest_handler_preserves_auxiliary_outputs(free_tcp_port):
    route = f"/rest-batch-aux-{free_tcp_port}"
    client_responses = []
    client_errors = []
    captured_audits = []

    class HandlerOutput(hg.TimeSeriesSchema):
        response: hg.TSD[int, hg.TS[RestResponse]]
        audit: hg.TS[str]

    @rest_handler(url=route, data_type=MyCS)
    @hg.compute_node
    def handler(
        request: hg.TSD[int, hg.TS[RestRequest]],
    ) -> hg.TSB[HandlerOutput]:
        modified = tuple(request.modified_items())
        return {
            "response": {
                request_id: RestDeleteResponse(
                    status=RestResultEnum.NOT_FOUND,
                    reason=f"missing:{request_value.value.id}",
                )
                for request_id, request_value in modified
            },
            "audit": ",".join(
                f"{type(request_value.value).__name__}:{request_value.value.id}"
                for _, request_value in modified
            ),
        }

    @hg.push_queue(hg.TS[bool])
    def drive_client(sender):
        def run():
            try:
                deadline = time.monotonic() + 10.0
                while True:
                    connection = http.client.HTTPConnection(
                        "127.0.0.1", free_tcp_port, timeout=2.0
                    )
                    try:
                        connection.request("DELETE", f"{route}/abc")
                        response = connection.getresponse()
                        client_responses.append((response.status, response.read()))
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

        Thread(target=run, name="hgraph-rest-batch-aux-handler-test", daemon=True).start()

    @hg.sink_node
    def capture(audit: hg.TS[str]) -> None:
        captured_audits.append(audit.value)

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
        outputs = handler()
        capture(outputs.audit)
        stop_when_done(done)

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert client_errors == []
    assert client_responses == [(404, b'{ "reason": "missing:abc" }')]
    assert captured_audits == ["RestDeleteRequest:abc"]
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())


def test_rest_client_helpers_round_trip_against_rest_handler(free_tcp_port):
    route = f"/rest-client-{free_tcp_port}"
    base_url = f"http://127.0.0.1:{free_tcp_port}{route}"
    captured = []

    @rest_handler(url=route, data_type=MyCS)
    @hg.compute_node
    def handler(request: hg.TS[RestRequest]) -> hg.TS[RestResponse]:
        value = request.value
        if isinstance(value, RestListRequest):
            return RestListResponse(status=RestResultEnum.OK, ids=("1", "2"))
        if isinstance(value, RestReadRequest):
            return RestReadResponse[MyCS](
                status=RestResultEnum.OK,
                id=value.id,
                value=MyCS(a=1, b="read"),
            )
        if isinstance(value, RestCreateRequest):
            return RestCreateResponse[MyCS](
                status=RestResultEnum.CREATED,
                id=value.id,
                value=value.value,
            )
        if isinstance(value, RestUpdateRequest):
            return RestUpdateResponse[MyCS](
                status=RestResultEnum.OK,
                id=value.id,
                value=value.value,
            )
        if isinstance(value, RestDeleteRequest):
            return RestDeleteResponse(status=RestResultEnum.OK)
        raise AssertionError(f"unexpected REST request {value!r}")

    @hg.sink_node
    def capture(
        listed: hg.TS[RestListResponse],
        read: hg.TS[RestReadResponse[MyCS]],
        created: hg.TS[RestCreateResponse[MyCS]],
        updated: hg.TS[RestUpdateResponse[MyCS]],
        deleted: hg.TS[RestDeleteResponse],
        _engine: hg.EvaluationEngineApi = None,
    ) -> None:
        captured.append(
            (
                listed.value,
                read.value,
                created.value,
                updated.value,
                deleted.value,
            )
        )
        _engine.request_engine_stop()

    @hg.graph
    def application() -> None:
        register_http_server_adaptor(free_tcp_port)
        register_rest_client()
        url = hg.const(base_url)
        identifier = hg.const("1")
        capture(
            rest_list(url),
            rest_read[MyCS](url, identifier),
            rest_create(url, identifier, hg.const(MyCS(a=2, b="create"))),
            rest_update(url, identifier, hg.const(MyCS(a=3, b="update"))),
            rest_delete(url, identifier),
        )

    state = hg.GlobalState()
    with hg.GlobalContext(state):
        hg.run_graph(
            application,
            end_time=datetime.now(timezone.utc).replace(tzinfo=None)
            + timedelta(seconds=5),
            run_mode=hg.EvaluationMode.REAL_TIME,
        )

    assert captured == [
        (
            RestListResponse(status=RestResultEnum.OK, ids=("1", "2")),
            RestReadResponse[MyCS](
                status=RestResultEnum.OK,
                id="1",
                value=MyCS(a=1, b="read"),
            ),
            RestCreateResponse[MyCS](
                status=RestResultEnum.CREATED,
                id="1",
                value=MyCS(a=2, b="create"),
            ),
            RestUpdateResponse[MyCS](
                status=RestResultEnum.OK,
                id="1",
                value=MyCS(a=3, b="update"),
            ),
            RestDeleteResponse(status=RestResultEnum.OK),
        )
    ]
    assert not any(key.startswith("http_client_adaptor://") for key in state.keys())
    assert not any(key.startswith("http_server_adaptor://") for key in state.keys())
