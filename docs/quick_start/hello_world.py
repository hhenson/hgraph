from hgraph import graph, run_graph, EvaluationMode, const, debug_print


@graph
def hello_world():
    c = const("World")
    debug_print("Hello", c)


run_graph(hello_world, run_mode=EvaluationMode.SIMULATION)
