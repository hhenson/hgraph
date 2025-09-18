from dataclasses import dataclass
from datetime import timedelta, datetime
from random import randrange
from threading import Thread

import pytest
from frozendict import frozendict

from hgraph import (
    CompoundScalar,
    graph,
    TS,
    convert,
    sink_node,
    combine,
    TIME_SERIES_TYPE,
    EvaluationMode,
    compute_node,
    TSD,
    STATE,
    GraphConfiguration,
    evaluate_graph,
)
from hgraph.adaptors.tornado import rest_list, register_rest_client, rest_read, rest_create, rest_update, rest_delete
from hgraph.adaptors.tornado._rest_handler import (
    RestListRequest,
    RestRequest,
    RestCreateRequest,
    RestReadRequest,
    RestUpdateRequest,
    RestDeleteRequest,
    RestResponse,
    RestListResponse,
    RestResultEnum,
    RestReadResponse,
    RestCreateResponse,
    RestUpdateResponse,
    RestDeleteResponse,
    rest_handler,
)
from hgraph.adaptors.tornado.http_server_adaptor import (
    HttpGetRequest,
    HttpRequest,
    HttpPostRequest,
    HttpPutRequest,
    HttpDeleteRequest,
    HttpResponse,
    http_server_handler,
    register_http_server_adaptor,
)
from hgraph import stop_engine
from hgraph.test import eval_node

URL = "http://localhost:8080/test"


@dataclass(frozen=True)
class MyCS(CompoundScalar):
    a: int
    b: str


@graph
def _convert_to_request(ts: TS[HttpRequest]) -> TS[RestRequest]:
    return convert[TS[RestRequest]](ts, value_type=MyCS)


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [HttpGetRequest(url=URL), RestListRequest(url=URL)],
        [HttpGetRequest(url=URL, url_parsed_args=("id1",)), RestReadRequest(url=URL, id="id1")],
        [
            HttpPostRequest(url=URL, body='{ "id": "id1", "value": { "a": 1, "b": "b" } }'),
            RestCreateRequest[MyCS](url=URL, id="id1", value=MyCS(a=1, b="b")),
        ],
        [
            HttpPutRequest(url=URL, url_parsed_args=("id1",), body='{ "a": 1, "b": "b" }'),
            RestUpdateRequest[MyCS](url=URL, id="id1", value=MyCS(a=1, b="b")),
        ],
        [
            HttpDeleteRequest(url=URL, url_parsed_args=("id1",)),
            RestDeleteRequest(url=URL, id="id1"),
        ],
    ],
)
def test_to_request(value, expected):
    result = eval_node(_convert_to_request, [value])
    assert result == [expected]


@graph
def _convert_from_rest_response(ts: TS[RestResponse]) -> TS[HttpResponse]:
    return convert[TS[HttpResponse]](ts)


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [
            RestListResponse(status=RestResultEnum.OK, ids=["1", "2"]),
            HttpResponse(
                status_code=200, headers=frozendict({"Content-Type": "application/json"}), body=b'[ "1", "2" ]'
            ),
        ],
        [
            RestReadResponse[MyCS](status=RestResultEnum.OK, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ],
        [
            RestCreateResponse[MyCS](status=RestResultEnum.CREATED, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=201,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ],
        [
            RestUpdateResponse[MyCS](status=RestResultEnum.OK, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "id": "1", "value": {"a": 1, "b": "b"} }',
            ),
        ],
        [
            RestDeleteResponse(status=RestResultEnum.OK),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b"",
            ),
        ],
        [
            RestDeleteResponse(status=RestResultEnum.NOT_FOUND, reason="Id not found"),
            HttpResponse(
                status_code=404,
                headers=frozendict({"Content-Type": "application/json"}),
                body=b'{ "reason": "Id not found" }',
            ),
        ],
    ],
)
def test_from_response(value, expected):
    result = eval_node(_convert_from_rest_response, [value])
    assert result == [expected]


@pytest.fixture(scope="function")
def port() -> int:
    return randrange(3300, 32000)


@pytest.mark.serial
def test_single_rest_request_graph(port):
    @rest_handler(url="/test_rest", data_type=MyCS)
    def x(request: TS[RestRequest]) -> TS[RestResponse]:
        return combine[TS[RestDeleteResponse]](status=RestResultEnum.NOT_FOUND, reason="Hello, world!")

    @http_server_handler(url="/stop_rest")
    def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
        stop_engine(request)
        return combine[TS[HttpResponse]](status_code=200, body=b"Ok")

    @sink_node
    def q(t: TIME_SERIES_TYPE):
        Thread(target=make_query).start()

    @graph
    def g():
        register_http_server_adaptor(port=port)
        q(True)

    response1 = None

    def make_query():
        import requests
        import time

        nonlocal response1

        time.sleep(0.1)

        response1 = requests.request("DELETE", f"http://localhost:{port}/test_rest/abc", timeout=1)
        time.sleep(0.5)
        requests.request("GET", f"http://localhost:{port}/stop_rest", timeout=1)

    evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME,
                                         end_time=datetime.utcnow() + timedelta(seconds=3)))

    assert response1 is not None
    assert response1.status_code == 404
    assert "Hello, world!" in response1.text


@pytest.mark.serial
def test_multiple_request_graph(port):
    @rest_handler(url="/test_multi", data_type=MyCS)
    @compute_node
    def x(request: TSD[int, TS[RestRequest]], _state: STATE = None) -> TSD[int, TS[RestResponse]]:
        out = {}
        for i, v in request.modified_items():
            v = v.value
            if isinstance(v, RestDeleteRequest):
                _state.counter_delete = _state.counter_delete + 1 if hasattr(_state, "counter_delete") else 0
                out[i] = RestDeleteResponse(
                    status=RestResultEnum.NOT_FOUND, reason=f"Hello, world #{_state.counter_delete}!"
                )
            else:
                out[i] = RestReadResponse[MyCS](
                    status=RestResultEnum.BAD_REQUEST, reason=f"Incorrect request type: {v}"
                )

        return out

    @http_server_handler(url="/stop_multi")
    def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
        stop_engine(request)
        return combine[TS[HttpResponse]](status_code=200, body=b"Ok")

    @sink_node
    def q(t: TIME_SERIES_TYPE):
        Thread(target=make_query).start()

    @graph
    def g():
        register_http_server_adaptor(port=port)
        q(True)

    response1 = None
    response2 = None

    def make_query():
        import requests
        import time

        nonlocal response1
        nonlocal response2

        time.sleep(0.1)

        response1 = requests.request("DELETE", f"http://localhost:{port}/test_multi/1", timeout=1)
        time.sleep(0.1)
        response2 = requests.request("DELETE", f"http://localhost:{port}/test_multi/1", timeout=1)
        time.sleep(0.1)
        requests.request("GET", f"http://localhost:{port}/stop_multi", timeout=1)

    evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME,
                                         end_time=datetime.utcnow() + timedelta(seconds=3)))

    assert response1 is not None
    assert response1.status_code == 404
    assert "Hello, world #0!" in response1.text
    assert response2 is not None
    assert response2.status_code == 404
    assert "Hello, world #1!" in response2.text


BASE_URL = "/test_rest_client"


@rest_handler(url=BASE_URL, data_type=MyCS)
@compute_node
def x(request: TS[RestRequest]) -> TS[RestResponse]:
    request = request.value
    if isinstance(request, RestListRequest):
        return RestListResponse(
            status=RestResultEnum.OK,
            ids=(
                "1",
                "2",
            ),
        )
    elif isinstance(request, RestReadRequest):
        return RestReadResponse[MyCS](status=RestResultEnum.OK, id=request.id, value=MyCS(a=1, b="a"))
    elif isinstance(request, RestCreateRequest):
        return RestCreateResponse[MyCS](status=RestResultEnum.CREATED, id=request.id, value=request.value)
    elif isinstance(request, RestUpdateRequest):
        return RestUpdateResponse[MyCS](status=RestResultEnum.OK, id=request.id, value=request.value)
    elif isinstance(request, RestDeleteRequest):
        return RestDeleteResponse(status=RestResultEnum.OK)
    else:
        return RestReadResponse(status=RestResultEnum.BAD_REQUEST, reason=f"Unknown Request {request}")


@graph
def g(port: int):
    register_rest_client()
    register_http_server_adaptor(port=port)


def test_rest_list_client(port):
    URL = f"http://localhost:{port}{BASE_URL}"
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

    @graph
    def rest_list_test() -> TS[tuple[str, ...]]:
        g(port)
        out = rest_list(URL)
        stop_engine(out)
        return out.ids

    result = evaluate_graph(rest_list_test, config)
    assert len(result) == 1
    assert result[0][1] == ("1", "2")


def test_rest_read_client(port):
    URL = f"http://localhost:{port}{BASE_URL}"
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

    @graph
    def rest_read_test() -> TS[MyCS]:
        g(port)
        out = rest_read[MyCS](URL, "1")
        stop_engine(out)
        return out.value

    assert evaluate_graph(rest_read_test, config)[0][1] == MyCS(a=1, b="a")


def test_rest_create_client(port):
    URL = f"http://localhost:{port}{BASE_URL}"
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

    @graph
    def rest_create_test() -> TS[MyCS]:
        g(port)
        out = rest_create(URL, "1", MyCS(a=1, b="a"))
        stop_engine(out)
        return out.value

    assert evaluate_graph(rest_create_test, config)[0][1] == MyCS(a=1, b="a")


def test_rest_update_client(port):
    URL = f"http://localhost:{port}{BASE_URL}"
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

    @graph
    def rest_update_test() -> TS[MyCS]:
        g(port)
        out = rest_update(URL, "1", MyCS(a=1, b="a"))
        stop_engine(out)
        return out.value

    assert evaluate_graph(rest_update_test, config)[0][1] == MyCS(a=1, b="a")


def test_rest_delete_client(port):
    URL = f"http://localhost:{port}{BASE_URL}"
    config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

    @graph
    def rest_delete_test() -> TS[RestResultEnum]:
        g(port)
        out = rest_delete(URL, "1")
        stop_engine(out)
        return out.status

    result = evaluate_graph(rest_delete_test, config)
    assert len(result) == 1
    assert result[0][1] == RestResultEnum.OK
