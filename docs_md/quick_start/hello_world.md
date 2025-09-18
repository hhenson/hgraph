Hello World
===========

The obligatory hello world example.

```python
from hgraph import graph, evaluate_graph, GraphConfiguration, EvaluationMode, const, debug_print

@graph
def hello_world() -> None:
    c = const("World")
    debug_print("Hello", c)

evaluate_graph(hello_world, GraphConfiguration(run_mode=EvaluationMode.SIMULATION))

>> [1970-01-01 00:00:00.000425][1970-01-01 00:00:00.000001] Hello: World
```

In this example we create a graph with a constant node (``c``) and then supply
this node to the debug_print node. The debug_print node will print the value.
We then evaluation the graph in simulation mode, which by default starts evaluation
at 1 micro-second past the unix epoch (1970-01-01 00:00:00.000000) and runs for 1 engine cycle.
As a result, we see one line of output from the debug_print node.

