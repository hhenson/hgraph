Graphs and Nodes
================

In HGraph there are three main types of nodes: source, compute, sink.

Source nodes are split into push and pull nodes. Both types of source nodes introduce
ticks (or data changes) into the graph. Pull nodes do so in a deterministic way, that
is they can be queried at the end of a run and know the next time they will be 
introducing a new tick. (They are scheduled). Push nodes introduce ticks in a non-deterministic
way, that is, they are not scheduled, and can introduce ticks at any time. These
source nodes typically represent asynchronous data sources, such as network sockets,
user intputs, etc. Whereas pull nodes represent synchronous data sources, such as
database queries, file reads, etc.

In a simulation graph, only PULL nodes are allowed. PUSH nodes are supported in a REAL-TIME
graph.

In Python, we have further simplified the API to support creating nodes that yield the
next value in a sequence. These nodes are called generators and are marked using the
`@generator` decorator.

An example of a generator node is the following:

```python
from hgraph import generator, TS, MIN_ST, MIN_TD

@generator
def counter(max_count: int) -> TS[int]:
    for i in range(max_count):
        yield MIN_ST + i * MIN_TD, i
```

The function implementing the `@generator` decorator must yield results of the form
`(timestamp, value)`. The timestamp must be a datetime (in UTC) object, and the value
must be value setter compatible with the defined output type (in this case``TS[int]``).

The source node cannot have any time-series inputs, but scalar values are allowed.
In this case the `max_count` parameter is a scalar value indicating the maximum number
of ticks the node will yield.

NOTE: The use of MIN_ST and MIN_TD. These are a set of useful constants that describe
certain characteristics regarding the time information of the evaluation graph.
MIN_ST is the minimum start time of the graph, MIN_TD is the smallest step time
the graph can take. There are other useful constants, such as MIN_DT, MAX_DT and MAX_ST.
These represent the smallest time representable in the graph, the largest time
representable, and the largest stop time of the graph.

The following is an example of a compute node:

```python
from hgraph import compute_node, TS, TS_OUT

@compute_node
def sum_time_series(ts: TS[int], _output: TS_OUT[int] = None) -> TS[int]:
    return _output.value + ts.value if _output.valid else ts.value
```

This node sums the inputs over time. The `_output` parameter is a special parameter that
allows the function to gain access to the output time-series. This is useful in this case
as the output is state that we would otherwise have to track internally.

The return from the function is either None or a value that is setter compatible with the
output type (in this case `TS[int]`). If the return is None, then the output is not updated.

Next an example of a sink node:

```python
from hgraph import sink_node, TS

@sink_node
def print_time_series(ts: TS[int]):
    print(ts.value)
```

A sink ends the flow of data, this is a leaf node in the graph. As such, the sink node
can only have time-series inputs, and no outputs. The function must return None.

In the example above, the sink node simply prints the value of the time-series.

Now it is not possible to just call the nodes directly, whilst they look like Python code,
they are not. They are required to belong to a graph. The graph allows us to connect
nodes together into a useful structure, all graphs have at least one source and one sink
node to be valid. Most graphs will also have `compute` nodes in them to be useful.

The following is an example of a graph:

```python
from hgraph import graph

@graph
def main():
    c = counter(10)
    s = sum_time_series(c)
    print_time_series(s)
```

This graph has no inputs and no outputs. This is only suitable for the top-level graph.
It is also only graphs that are allowed to be constructed in this way. The graph, when
evaluated, will track the sink nodes constructed and then compute the final shape by
traversing the graph backwards from sink to source. 

Any `compute` or `source` nodes that are not reachable from a `sink` node in the graph are
wired out of the graph. For example, if we had not connected ``s`` to the ``print_time_series``
sink node, then the graph would have been empty.

Once again, the graph is not a Python function, it is a graph object. It is only when
the graph is evaluated by the graph builder that the graph is constructed.

The general form of running the graph is to call the ``run_graph`` function:

```python
from hgraph import run_graph

run_graph(main)
```

This will build the graph (*main*) and then run it in simulation mode.

The graph building step is done prior to running the graph. To see how this looks
add a few ``print`` statements, or put a debug point, into the code and see how it works.

What you should see is the graph run, and then the source, compute and sink nodes being called
after the graph has been built. The nodes will be called for as many times as a source
node introduces a tick into the graph.
