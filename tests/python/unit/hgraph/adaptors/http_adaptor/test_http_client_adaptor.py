from frozendict import frozendict

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
        get_recorded_value,
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
        @http_server_handler(url="/test/(.*)")
        def x(request: TS[HttpRequest]) -> TS[HttpResponse]:
            return combine[TS[HttpResponse]](status_code=200, body=request.url_parsed_args[0])

        @graph
        def g():
            register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=8081)
            register_adaptor(None, http_client_adaptor_impl)

            queries = frozendict({
                "one": HttpRequest("http://localhost:8081/test/one"),
                "two": HttpRequest("http://localhost:8081/test/two"),
            })

            record(
                map_(
                    lambda key, q: key == http_client_adaptor(q).body,
                    q=const(queries, tp=TSD[str, TS[HttpRequest]], delay=timedelta(milliseconds=10)),
                )
            )

        with GlobalState():
            run_graph(g, run_mode=EvaluationMode.REAL_TIME, end_time=timedelta(seconds=3))
            for tick in [{"one": True}, {"two": True}]:
                assert tick in [t[-1] for t in get_recorded_value()]

except ImportError:
    pass
