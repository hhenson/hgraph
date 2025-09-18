from hgraph import graph, evaluate_graph, GraphConfiguration, EvaluationMode, const, debug_print


@graph
def hello_world():
    c = const("World")
    debug_print("Hello", c)


evaluate_graph(hello_world, GraphConfiguration(run_mode=EvaluationMode.SIMULATION))
