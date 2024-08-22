from collections import defaultdict

from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.websocket_server_adaptor import (
    websocket_server_handler,
    WebSocketRequest,
    WebSocketResponse,
    websocket_server_adaptor_impl,
    websocket_server_adaptor_helper,
)

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
        TSB,
        GlobalState,
        sample,
        convert,
    )
    from hgraph.adaptors.tornado.http_server_adaptor import (
        http_server_handler,
        HttpRequest,
        HttpResponse,
        http_server_adaptor_impl,
        http_server_adaptor,
        http_server_adaptor_helper,
    )
    from hgraph.nodes import stop_engine

    @graph
    def run_test(queries: dict[object, object]):
        def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
            stop_engine(request)
            return combine[TS[HttpResponse]](status_code=200, body="Ok")

        http_server_handler(url="/stop")(s)

        @sink_node
        def q(t: TIME_SERIES_TYPE):
            Thread(target=make_query).start()

        def make_query():
            import tornado
            import requests
            import time

            time.sleep(0.1)

            async def ws(i, msg):
                if isinstance(i, str):
                    ws1 = await tornado.websocket.websocket_connect(f"ws://localhost:8082/test/{i}", connect_timeout=1)
                else:
                    ws1 = await tornado.websocket.websocket_connect("ws://localhost:8082/test", connect_timeout=1)
                ws1.write_message(msg, binary=True)
                GlobalState().instance().responses[i] = await ws1.read_message()

            for id, msg in queries.items():
                TornadoWeb.instance().get_loop().add_callback(ws, id, msg)
                time.sleep(0.1)

            sleeps = 0
            while len(GlobalState().instance().responses) < len(queries) and sleeps < 10:
                time.sleep(0.1)
                sleeps += 1

            requests.request("GET", "http://localhost:8081/stop", timeout=1)

        q(True)

    def test_single_websocket_request_graph():
        @websocket_server_handler(url="/test")
        def x(request: TSB[WebSocketRequest]) -> TSB[WebSocketResponse]:
            return combine[TSB[WebSocketResponse]](
                connect_response=True,
                message=request.message,
            )

        @graph
        def g():
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=8081)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=8082)
            run_test(queries={1: b"Hello, world!", 2: b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

            assert gs.responses[1] == b"Hello, world!"
            assert gs.responses[2] == b"Hello, world again!"

    def test_multiple_websocket_request_graph():
        @websocket_server_handler(url="/test")
        @compute_node
        def x(request: TSD[int, TSB[WebSocketRequest]], _state: STATE = None) -> TSD[int, TSB[WebSocketResponse]]:
            out = defaultdict(dict)
            for i, v in request.modified_items():
                if v.connect_request.modified:
                    out[i]["connect_response"] = True
                if v.message.modified:
                    _state.counter = _state.counter + 1 if hasattr(_state, "counter") else 0
                    out[i]["message"] = f"Hello, world #{_state.counter}!".encode()

            return out

        @graph
        def g():
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=8081)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=8082)
            run_test(queries={1: b"Hello, world!", 2: b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

            assert gs.responses[1] == b"Hello, world #0!"
            assert gs.responses[2] == b"Hello, world #1!"

    def test_websocket_server_adaptor_graph():
        @websocket_server_handler(url="/test/(.*)")
        def x(request: TSB[WebSocketRequest], b: TS[int]) -> TSB[WebSocketResponse]:
            return combine[TSB[WebSocketResponse]](
                connect_response=True,
                message=convert[TS[bytes]](
                    format_("Hello, {} and {}!", request.connect_request.url_parsed_args[0], sample(request.message, b))
                ),
            )

        @graph
        def g():
            register_adaptor(None, websocket_server_adaptor_helper, port=8082)
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=8081)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=8082)
            x(b=33)
            run_test(queries={"a": b"Hello, world!", "b": b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))

            assert gs.responses["a"] == b"Hello, a and 33!"
            assert gs.responses["b"] == b"Hello, b and 33!"

except ImportError:
    pass
