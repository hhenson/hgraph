from hgraph import graph, compute_node, TS, STATE, register_adaptor, TSD
from hgraph.adaptors.tornado.http_server_adaptor import http_server_handler, HttpRequest, HttpResponse, \
    http_server_adaptor_helper, http_server_adaptor_impl
from hgraph.test import EvaluationTrace


@graph
def trace_controller(port: int = 8090):
    @compute_node
    def control_trace(request: TSD[int, TS[HttpRequest]], _state: STATE = None) -> TSD[int, TS[HttpResponse]]:
        out = {}
        for i, r in request.modified_items():
            r: HttpRequest = r.value
            if not r.url_parsed_args or (command := r.url_parsed_args[0]) not in ("start", "stop"):
                return HttpResponse(status_code=400, body="Invalid command")

            options = r.query
            options = {
                "start": options.get("start", None),
                "eval": options.get("eval", None),
                "stop": options.get("stop", None),
                "node": options.get("node", None),
                "graph": options.get("graph", None),
                "filter": options.get("filter", None),
            }
            if (invalid := [o for o in r.query if o not in options]):
                out[i] = HttpResponse(status_code=400, body=f"Invalid options {invalid}")
                continue

            options = {k: bool(v) if k != 'filter' else v for k, v in options.items() if v is not None}

            if _state.tracer is not None:
                request.owning_graph.evaluation_engine.remove_life_cycle_observer(_state.tracer)
                _state.tracer = None

            if command == "start":
                _state.tracer = EvaluationTrace(**options)
                request.owning_graph.evaluation_engine.add_life_cycle_observer(_state.tracer)

            out[i] = HttpResponse(status_code=200, body=f"{command} succeeded with options {options}")

        return out

    @control_trace.start
    def start_trace(_state: STATE):
        _state.tracer = None


    http_server_handler(control_trace, url=f"/trace/(.*)")

    register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)
