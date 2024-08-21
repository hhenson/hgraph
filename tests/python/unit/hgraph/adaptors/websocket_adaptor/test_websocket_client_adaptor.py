from typing import Tuple

from frozendict import frozendict

from hgraph.adaptors.tornado.http_client_adaptor import http_client_adaptor_impl, http_client_adaptor
from hgraph.adaptors.tornado.websocket_client_adaptor import websocket_client_adaptor, websocket_client_adaptor_impl
from hgraph.adaptors.tornado.websocket_server_adaptor import websocket_server_handler, WebSocketRequest, \
    WebSocketResponse, websocket_server_adaptor_impl, WebSocketConnectRequest

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
        const,
        record,
        GlobalState,
        get_recorded_value, TSB, sample, convert, emit, gate, feedback,
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

    def test_single_request_graph_client():
        @websocket_server_handler(url="/test/(.*)")
        def x(request: TSB[WebSocketRequest]) -> TSB[WebSocketResponse]:
            return combine[TSB[WebSocketResponse]](
                connect_response=True,
                message=convert[TS[bytes]](
                    format_("Hello, {}, {}!", request.connect_request.url_parsed_args[0], convert[TS[str]](request.message))
                ),
            )

        @graph
        def g():
            register_adaptor("websocket_server_adaptor", websocket_server_adaptor_impl, port=8082)
            register_adaptor(None, websocket_client_adaptor_impl)

            queries = frozendict({
                "one": (WebSocketConnectRequest("ws://localhost:8082/test/one"), (b"message 1", b"message 2")),
                "two": (WebSocketConnectRequest("ws://localhost:8082/test/two"), (b"message X", b"message Y")),
            })

            @graph
            def ws_client(i: TS[Tuple[WebSocketConnectRequest, Tuple[bytes, ...]]]) -> TS[bytes]:
                connected = feedback(TS[bool])
                resp = websocket_client_adaptor(combine[TSB[WebSocketRequest]](
                    connect_request=i[0], message=gate(connected(), emit(i[1]))))
                connected(resp.connect_response)
                return resp.message

            record(
                map_(
                    ws_client,
                    i=const(queries, tp=TSD[str, TS[Tuple[WebSocketConnectRequest, Tuple[bytes, ...]]]], delay=timedelta(milliseconds=10)),
                ),
            )

        with GlobalState():
            run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3), __trace__=True)
            for tick in [{"one": b"Hello, one, message 1!"}, {"one": b"Hello, one, message 2!"},
                         {"two": b"Hello, two, message X!"}, {"two": b"Hello, two, message Y!"}]:
                assert frozendict(tick) in [t[-1] for t in get_recorded_value()]

except ImportError:
    pass
