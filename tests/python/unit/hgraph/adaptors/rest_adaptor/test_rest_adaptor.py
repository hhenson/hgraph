from dataclasses import dataclass
from datetime import timedelta
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
    register_adaptor,
    run_graph,
    EvaluationMode,
)
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
    http_server_adaptor_impl,
)
from hgraph.nodes import stop_engine
from hgraph.test import eval_node

URL = "http://localhost:8080/test"


@dataclass(frozen=True)
class MyCS(CompoundScalar):
    a: int
    b: str


@graph
def _convert_to_request(ts: TS[HttpRequest]) -> TS[RestRequest[MyCS]]:
    return convert[TS[RestRequest[MyCS]]](ts)


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [HttpGetRequest(url=URL), RestListRequest[RestRequest[MyCS]](url=URL)],
        [HttpGetRequest(url=URL, url_parsed_args=("id1",)), RestReadRequest[RestRequest[MyCS]](url=URL, id="id1")],
        [
            HttpPostRequest(url=URL, body='{ "id": "id1", "value": { "a": 1, "b": "b" } }'),
            RestCreateRequest[RestRequest[MyCS], MyCS](url=URL, id="id1", value=MyCS(a=1, b="b")),
        ],
        [
            HttpPutRequest(url=URL, url_parsed_args=("id1",), body='{ "a": 1, "b": "b" }'),
            RestUpdateRequest[RestRequest[MyCS], MyCS](url=URL, id="id1", value=MyCS(a=1, b="b")),
        ],
        [
            HttpDeleteRequest(url=URL, url_parsed_args=("id1",)),
            RestDeleteRequest[RestRequest[MyCS]](url=URL, id="id1"),
        ],
    ],
)
def test_to_request(value, expected):
    result = eval_node(_convert_to_request, [value])
    assert result == [expected]


@graph
def _convert_from_rest_response(ts: TS[RestResponse[MyCS]]) -> TS[HttpResponse]:
    return convert[TS[HttpResponse]](ts)


@pytest.mark.parametrize(
    ["value", "expected"],
    [
        [
            RestListResponse[RestResponse[MyCS]](status=RestResultEnum.OK, ids=["1", "2"]),
            HttpResponse(
                status_code=200, headers=frozendict({"Content-Type": "application/json"}), body='[ "1", "2" ]'
            ),
        ],
        [
            RestReadResponse[RestResponse[MyCS], MyCS](status=RestResultEnum.OK, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body='{ "id": "1", "value": { "a": 1, "b": "b" } }',
            ),
        ],
        [
            RestCreateResponse[RestResponse[MyCS], MyCS](status=RestResultEnum.CREATED, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=201,
                headers=frozendict({"Content-Type": "application/json"}),
                body='{ "id": "1", "value": { "a": 1, "b": "b" } }',
            ),
        ],
        [
            RestUpdateResponse[RestResponse[MyCS], MyCS](status=RestResultEnum.OK, id="1", value=MyCS(a=1, b="b")),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body='{ "id": "1", "value": { "a": 1, "b": "b" } }',
            ),
        ],
        [
            RestDeleteResponse[RestResponse[MyCS]](status=RestResultEnum.OK),
            HttpResponse(
                status_code=200,
                headers=frozendict({"Content-Type": "application/json"}),
                body="",
            ),
        ],
        [
            RestDeleteResponse[RestResponse[MyCS]](status=RestResultEnum.NOT_FOUND, reason="Id not found"),
            HttpResponse(
                status_code=404,
                headers=frozendict({"Content-Type": "application/json"}),
                body='{ "reason": "Id not found" }',
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
    def x(request: TS[RestRequest[MyCS]]) -> TS[RestResponse[MyCS]]:
        return combine[TS[RestDeleteResponse[RestResponse[MyCS]]]](
            status=RestResultEnum.NOT_FOUND, reason="Hello, world!"
        )

    @http_server_handler(url="/stop_rest")
    def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
        stop_engine(request)
        return combine[TS[HttpResponse]](status_code=200, body="Ok")

    @sink_node
    def q(t: TIME_SERIES_TYPE):
        Thread(target=make_query).start()

    @graph
    def g():
        register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)
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

    run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1))

    assert response1 is not None
    assert response1.status_code == 404
    assert "Hello, world!" in response1.text
