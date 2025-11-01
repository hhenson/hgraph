from hgraph import graph, compute_node, TS, STATE, register_adaptor, TSD
from hgraph.adaptors.tornado.http_server_adaptor import (
    http_server_handler,
    HttpRequest,
    HttpResponse,
    http_server_adaptor_impl,
)
from hgraph.test import EvaluationTrace


@graph
def trace_controller(port: int = 8090):
    form = """
    <html><body>
    <h1>Trace Controller</h1>
    <p>Start or stop the evaluation trace</p>
    <p>{result}</p>
    <form action="/trace/start">
        <input type="checkbox" id="start" name="start" value="True" {start}>
        <label for="start">Start</label><br/>
        <input type="checkbox" id="eval" name="eval" value="True" {eval}>
        <label for="eval">Eval</label><br/>
        <input type="checkbox" id="stop" name="stop" value="True" {stop}>
        <label for="stop">Stop</label><br/>
        <input type="checkbox" id="node" name="node" value="True" {node}>
        <label for="node">Node</label><br/>
        <input type="checkbox" id="graph" name="graph" value="True" {graph}>
        <label for="graph">Graph</label><br/>
        <input type="text" id="filter" name="filter" value="{filter}">
        <label for="filter">Filter</label><br/>
        <input type="submit" value="start">
    </form>
    <form action="/trace/stop">
        <input type="submit" value="stop">
    </form>
    </body></html>
    """

    @compute_node
    def control_trace(request: TSD[int, TS[HttpRequest]], _state: STATE = None) -> TSD[int, TS[HttpResponse]]:
        out = {}
        for i, r in request.modified_items():
            r: HttpRequest = r.value

            current_options = {
                "start": ("checked" if _state.tracer.start else "") if _state.tracer else "",
                "eval": ("checked" if _state.tracer.eval else "") if _state.tracer else "",
                "stop": ("checked" if _state.tracer.stop else "") if _state.tracer else "",
                "node": ("checked" if _state.tracer.node else "") if _state.tracer else "",
                "graph": ("checked" if _state.tracer.graph else "") if _state.tracer else "",
                "filter": _state.tracer.filter if _state.tracer else "",
            }

            if not r.url_parsed_args:
                out[i] = HttpResponse(status_code=200, body=form.format(result="", **current_options))

            if (command := r.url_parsed_args[0]) not in ("start", "stop"):
                out[i] = HttpResponse(status_code=400, body=form.format(result="Invalid command", **current_options))

            options = r.query
            options = {
                "start": options.get("start", False),
                "eval": options.get("eval", False),
                "stop": options.get("stop", False),
                "node": options.get("node", False),
                "graph": options.get("graph", False),
                "filter": options.get("filter", ""),
            }
            if invalid := [o for o in r.query if o not in options]:
                out[i] = HttpResponse(status_code=400, body=f"Invalid options {invalid}")
                continue

            options = {k: bool(v) if k != "filter" else v for k, v in options.items() if v is not None}

            if _state.tracer is not None:
                request.owning_graph.evaluation_engine.remove_life_cycle_observer(_state.tracer)
                _state.tracer = None

                out[i] = HttpResponse(status_code=200, body=form.format(result="Stop succeeded", **current_options))

            if command == "start":
                _state.tracer = EvaluationTrace(**options)
                request.owning_graph.evaluation_engine.add_life_cycle_observer(_state.tracer)

                options = {
                    k: ("checked" if bool(v) else "") if k != "filter" else v
                    for k, v in options.items()
                    if v is not None
                }
                out[i] = HttpResponse(status_code=200, body=form.format(result=f"Start succeeded", **options))

        return out

    @control_trace.start
    def start_trace(_state: STATE):
        _state.tracer = None

    http_server_handler(control_trace, url=f"/trace/(.*)")

    register_adaptor("http_server_adaptor", http_server_adaptor_impl, port=port)
