from collections import defaultdict
from random import randrange

import pytest
from frozendict import frozendict

from hgraph.adaptors.tornado._tornado_web import TornadoWeb
from hgraph.adaptors.tornado.websocket_client_adaptor import websocket_client_adaptor, websocket_client_adaptor_impl
from hgraph.adaptors.tornado.websocket_server_adaptor import (
    WebSocketClientRequest,
    websocket_server_handler,
    WebSocketServerRequest,
    WebSocketResponse,
    websocket_server_adaptor_impl,
    websocket_server_adaptor_helper,
    WebSocketConnectRequest,
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
        get_recorded_value,
        const,
        record,
        gate,
        emit,
        feedback,
        GraphConfiguration,
        evaluate_graph,
    )
    from hgraph.adaptors.tornado.http_server_adaptor import (
        http_server_handler,
        HttpRequest,
        HttpResponse,
        http_server_adaptor_impl,
        http_server_adaptor,
        http_server_adaptor_helper,
    )
    from hgraph import stop_engine

    PORT = randrange(3300, 32000)

    @graph
    def run_test(queries: dict[object, object]):
        def s(request: TS[HttpRequest]) -> TS[HttpResponse]:
            stop_engine(request)
            return combine[TS[HttpResponse]](status_code=200, body=b"Ok")

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
                    ws1 = await tornado.websocket.websocket_connect(
                        f"ws://localhost:{PORT+1}/test/{i}", connect_timeout=1
                    )
                else:
                    ws1 = await tornado.websocket.websocket_connect(f"ws://localhost:{PORT+1}/test", connect_timeout=1)
                ws1.write_message(msg, binary=True)
                GlobalState().instance().responses[i] = await ws1.read_message()

            for id, msg in queries.items():
                TornadoWeb.instance().get_loop().add_callback(ws, id, msg)
                time.sleep(0.1)

            sleeps = 0
            while len(GlobalState().instance().responses) < len(queries) and sleeps < 10:
                time.sleep(0.1)
                sleeps += 1

            requests.request("GET", f"http://localhost:{PORT}/stop", timeout=1)

        q(True)

    @pytest.mark.xfail(reason="Does not run with xdist correctly")
    @pytest.mark.serial
    def test_single_websocket_request_graph():
        @websocket_server_handler(url="/test")
        def x(request: TSB[WebSocketServerRequest[bytes]]) -> TSB[WebSocketResponse[bytes]]:
            return combine[TSB[WebSocketResponse[bytes]]](
                connect_response=True,
                message=request.messages[-1],
            )

        @graph
        def g():
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=PORT)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=PORT + 1)
            run_test(queries={1: b"Hello, world!", 2: b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))

            assert gs.responses[1] == b"Hello, world!"
            assert gs.responses[2] == b"Hello, world again!"

    @pytest.mark.xfail(reason="Does not run with xdist correctly")
    @pytest.mark.serial
    def test_multiple_websocket_request_graph():
        @websocket_server_handler(url="/test")
        @compute_node
        def x(request: TSD[int, TSB[WebSocketServerRequest[bytes]]], _state: STATE = None) -> TSD[int, TSB[WebSocketResponse[bytes]]]:
            out = defaultdict(dict)
            for i, v in request.modified_items():
                if v.connect_request.modified:
                    out[i]["connect_response"] = True
                if v.messages.modified:
                    _state.counter = _state.counter + len(v.messages.value) if hasattr(_state, "counter") else 0
                    out[i]["message"] = f"Hello, world #{_state.counter}!".encode()

            return out

        @graph
        def g():
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=PORT)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=PORT + 1)
            run_test(queries={1: b"Hello, world!", 2: b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))

            assert gs.responses[1] == b"Hello, world #0!"
            assert gs.responses[2] == b"Hello, world #1!"

    @pytest.mark.xfail(reason="When running with all tests, the server does not start and the test then fails")
    def test_websocket_server_adaptor_graph():
        @websocket_server_handler(url="/test/(.*)")
        def x(request: TSB[WebSocketServerRequest[bytes]], b: TS[int]) -> TSB[WebSocketResponse[bytes]]:
            return combine[TSB[WebSocketResponse[bytes]]](
                connect_response=True,
                message=convert[TS[bytes]](
                    format_(
                        "Hello, {} and {}!", request.connect_request.url_parsed_args[0], sample(request.messages, b)
                    )
                ),
            )

        @graph
        def g():
            register_adaptor(None, websocket_server_adaptor_helper, port=PORT + 1)
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=PORT)
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=PORT + 1)
            x(b=33)
            run_test(queries={"a": b"Hello, world!", "b": b"Hello, world again!"})

        with GlobalState() as gs:
            gs.responses = {}

            evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3)))

            assert gs.responses["a"] == b"Hello, a and 33!"
            assert gs.responses["b"] == b"Hello, b and 33!"

    @pytest.mark.serial
    def test_single_request_graph_client():
        @websocket_server_handler(url="/test/(.*)")
        def x(request: TSB[WebSocketServerRequest[bytes]]) -> TSB[WebSocketResponse[bytes]]:
            return combine[TSB[WebSocketResponse[bytes]]](
                connect_response=True,
                message=convert[TS[bytes]](
                    format_(
                        "Hello, {}, {}!",
                        request.connect_request.url_parsed_args[0],
                        convert[TS[str]](emit(request.messages)),
                    )
                ),
            )

        @graph
        def g():
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=PORT + 1)
            register_adaptor(None, websocket_client_adaptor_impl)

            queries = frozendict({
                "one": (WebSocketConnectRequest(f"ws://localhost:{PORT+1}/test/one"), (b"message 1", b"message 2")),
                "two": (WebSocketConnectRequest(f"ws://localhost:{PORT+1}/test/two"), (b"message X", b"message Y")),
            })

            @graph
            def ws_client(i: TS[tuple[WebSocketConnectRequest, tuple[bytes, ...]]]) -> TS[bytes]:
                connected = feedback(TS[bool])
                resp = websocket_client_adaptor(
                    combine[TSB[WebSocketClientRequest[bytes]]](connect_request=i[0], message=gate(connected(), emit(i[1])))
                )
                connected(resp.connect_response)
                return resp.message

            record(
                map_(
                    ws_client,
                    i=const(
                        queries,
                        tp=TSD[str, TS[tuple[WebSocketConnectRequest, tuple[bytes, ...]]]],
                        delay=timedelta(milliseconds=10),
                    ),
                ),
            )

        with GlobalState():
            evaluate_graph(g, GraphConfiguration(run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=1)))
            value = get_recorded_value()
            result = {"one": [], "two": []}
            for _, d in value:
                if "one" in d:
                    result["one"].append(d["one"])
                if "two" in d:
                    result["two"].append(d["two"])
            expected = {
                "one": [b"Hello, one, message 1!", b"Hello, one, message 2!"],
                "two": [b"Hello, two, message X!", b"Hello, two, message Y!"],
            }
            assert result == expected

except ImportError:
    pass
