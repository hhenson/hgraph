from random import randrange

import pytest
from frozendict import frozendict

from hgraph import convert
from hgraph.adaptors.tornado.http_client_adaptor import http_client_adaptor_impl, http_client_adaptor

try:
    # the adaptors are optional so if the dependencies are not installed, skip  the tests
    import tornado
    import requests

    from datetime import timedelta
    from threading import Thread

    from hgraph import (
        TS,
        combine,
        graph,
        register_adaptor,
        EvaluationMode,
        sink_node,
        TIME_SERIES_TYPE,
        TSD,
        compute_node,
        STATE,
        format_,
        map_,
        record,
        const,
        GlobalState,
        get_recorded_value,
        GraphConfiguration,
        evaluate_graph,
        log_,
        default_path,
    )
    from hgraph.adaptors.tornado.http_server_adaptor import (
        http_server_handler,
        HttpRequest,
        HttpResponse,
        http_server_adaptor,
        HttpGetRequest,
        register_http_server_adaptor,
    )
    from hgraph import stop_engine

    @pytest.fixture(scope="function")
    def port() -> int:
        return randrange(3300, 32000)

    @pytest.mark.serial
    def test_single_request_graph(port):
        @http_server_handler(url="/test_http")
        def x(request: TS[HttpRequest]) -> TS[HttpResponse]:
            return combine[TS[HttpResponse]](status_code=200, body=b"Hello, world!")

        @http_server_handler(url="/stop_http")
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

            response1 = requests.request("GET", f"http://localhost:{port}/test_http", timeout=1)
            response2 = requests.request("GET", f"http://localhost:{port}/test_http", timeout=1)
            requests.request("GET", f"http://localhost:{port}/stop_http", timeout=1)

        evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, world!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, world!"

    @pytest.mark.xfail(reason="This test is flaky when run in CICD to build pip", strict=False)
    @pytest.mark.serial
    def test_multiple_request_graph(port):
        @http_server_handler(url="/test_multiple_request")
        @compute_node
        def x(request: TSD[int, TS[HttpRequest]], _state: STATE = None) -> TSD[int, TS[HttpResponse]]:
            out = {}
            for i, v in request.modified_items():
                _state.counter = _state.counter + 1 if hasattr(_state, "counter") else 0
                out[i] = HttpResponse(status_code=200, body=f"Hello, world #{_state.counter}!".encode())

            return out

        @http_server_handler(url="/stop_multiple_request")
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

            response1 = requests.request("GET", f"http://localhost:{port}/test_multiple_request", timeout=1)
            response2 = requests.request("GET", f"http://localhost:{port}/test_multiple_request", timeout=1)
            requests.request("GET", f"http://localhost:{port}/stop_multiple_request", timeout=1)

        evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, world #0!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, world #1!"

    @pytest.mark.serial
    def test_http_server_adaptor_graph(port):
        @sink_node
        def q(t: TIME_SERIES_TYPE):
            Thread(target=make_query).start()

        @http_server_handler(url="/test/(.*)")
        def x(request: TS[HttpRequest], b: TS[int]) -> TS[HttpResponse]:
            return combine[TS[HttpResponse]](
                status_code=200, body=convert[TS[bytes]](format_("Hello, {} and {}!", request.url_parsed_args[0], b))
            )

        @graph
        def g():
            register_http_server_adaptor(port=port)

            x(b=12)

            q(True)

        @http_server_handler(url="/stop")
        def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
            stop_engine(request)
            return combine[TS[HttpResponse]](status_code=200, body=b"Ok")

        response1 = None
        response2 = None

        def make_query():
            import requests
            import time

            nonlocal response1
            nonlocal response2

            time.sleep(0.1)

            response1 = requests.request("GET", f"http://localhost:{port}/test/one", timeout=1)
            response2 = requests.request("GET", f"http://localhost:{port}/test/two", timeout=1)
            requests.request("GET", f"http://localhost:{port}/stop", timeout=1)

        evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, one and 12!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, two and 12!"

    @pytest.mark.serial
    def test_single_request_graph_client(port):
        @http_server_handler(url="/test/(.*)")
        def x(request: TS[HttpRequest]) -> TS[HttpResponse]:
            return combine[TS[HttpResponse]](status_code=200, body=convert[TS[bytes]](request.url_parsed_args[0]))

        @graph
        def g():
            register_http_server_adaptor(port=port)
            register_adaptor("http_client", http_client_adaptor_impl)

            queries = frozendict({str(i): HttpGetRequest(f"http://localhost:{port}/test/{i}") for i in range(10)})

            @graph
            def _send_query(key: TS[str], q: TS[HttpRequest]) -> TS[bool]:
                out = http_client_adaptor(q)
                log_("Response: {}", out)
                return key == convert[TS[str]](out.body)

            record(
                map_(
                    _send_query,
                    q=const(queries, tp=TSD[str, TS[HttpRequest]], delay=timedelta(milliseconds=2)),
                )
            )

        with GlobalState():
            config = GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1))
            evaluate_graph(g, config)
            values = get_recorded_value()
            v = values[0][1]
            for _, d in values:
                v |= d
            assert v == {str(i): True for i in range(10)}

except ImportError:
    pass
