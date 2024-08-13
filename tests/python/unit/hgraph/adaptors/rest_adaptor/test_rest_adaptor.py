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
        run_graph,
        EvaluationMode,
        sink_node,
        TIME_SERIES_TYPE,
        TSD,
        compute_node,
        STATE,
        format_,
        map_,
    )
    from hgraph.adaptors.tornado.rest_adaptor import (
        rest_handler,
        RestRequest,
        RestResponse,
        rest_adaptor_impl,
        rest_adaptor,
        rest_adaptor_helper,
    )
    from hgraph.nodes import stop_engine

    def test_single_request_graph():
        @rest_handler(url="/test")
        def x(request: TS[RestRequest]) -> TS[RestResponse]:
            return combine[TS[RestResponse]](status_code=200, body="Hello, world!")

        @rest_handler(url="/stop")
        def s(request: TS[RestRequest]) -> TS[RestResponse]:
            stop_engine(request)
            return combine[TS[RestResponse]](status_code=200, body="Ok")

        @sink_node
        def q(t: TIME_SERIES_TYPE):
            Thread(target=make_query).start()

        @graph
        def g():
            register_adaptor("rest_adaptor", rest_adaptor_impl, port=8081)
            q(True)

        response1 = None
        response2 = None

        def make_query():
            import requests
            import time

            nonlocal response1
            nonlocal response2

            time.sleep(0.1)

            response1 = requests.request("GET", "http://localhost:8081/test", timeout=1)
            response2 = requests.request("GET", "http://localhost:8081/test", timeout=1)
            requests.request("GET", "http://localhost:8081/stop", timeout=1)

        run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, world!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, world!"

    def test_multiple_request_graph():
        @rest_handler(url="/test")
        @compute_node
        def x(request: TSD[int, TS[RestRequest]], _state: STATE = None) -> TSD[int, TS[RestResponse]]:
            out = {}
            for i, v in request.modified_items():
                _state.counter = _state.counter + 1 if hasattr(_state, "counter") else 0
                out[i] = RestResponse(status_code=200, body=f"Hello, world #{_state.counter}!")

            return out

        @rest_handler(url="/stop")
        def s(request: TS[RestRequest]) -> TS[RestResponse]:
            stop_engine(request)
            return combine[TS[RestResponse]](status_code=200, body="Ok")

        @sink_node
        def q(t: TIME_SERIES_TYPE):
            Thread(target=make_query).start()

        @graph
        def g():
            register_adaptor("rest_adaptor", rest_adaptor_impl, port=8081)
            q(True)

        response1 = None
        response2 = None

        def make_query():
            import requests
            import time

            nonlocal response1
            nonlocal response2

            time.sleep(0.1)

            response1 = requests.request("GET", "http://localhost:8081/test", timeout=1)
            response2 = requests.request("GET", "http://localhost:8081/test", timeout=1)
            requests.request("GET", "http://localhost:8081/stop", timeout=1)

        run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, world #0!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, world #1!"

    def test_rest_adaptor_graph():
        @sink_node
        def q(t: TIME_SERIES_TYPE):
            Thread(target=make_query).start()

        @rest_handler(url="/test/(.*)")
        def x(request: TS[RestRequest], b: TS[int]) -> TS[RestResponse]:
            return combine[TS[RestResponse]](
                status_code=200, body=format_("Hello, {} and {}!", request.url_parsed_args[0], b)
            )

        @graph
        def g():
            register_adaptor(None, rest_adaptor_helper, port=8081)

            x(b=12)

            q(True)

        @rest_handler(url="/stop")
        def s(request: TS[RestRequest]) -> TS[RestResponse]:
            stop_engine(request)
            return combine[TS[RestResponse]](status_code=200, body="Ok")

        response1 = None
        response2 = None

        def make_query():
            import requests
            import time

            nonlocal response1
            nonlocal response2

            time.sleep(0.1)

            response1 = requests.request("GET", "http://localhost:8081/test/one", timeout=1)
            response2 = requests.request("GET", "http://localhost:8081/test/two", timeout=1)
            requests.request("GET", "http://localhost:8081/stop", timeout=1)

        run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

        assert response1 is not None
        assert response1.status_code == 200
        assert response1.text == "Hello, one and 12!"
        assert response2 is not None
        assert response2.status_code == 200
        assert response2.text == "Hello, two and 12!"

except ImportError:
    pass
