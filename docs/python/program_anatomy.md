HGraph Program Anatomy
======================

HGraph has two key types of functions, namely graph and nodes. The graph describes
the wiring logic (or how nodes are to be connected), and the nodes describe
the actions to be performed on the data flow.

All HGraph functions use the following syntax to describe the shape of the component:

[Component Signature](component_signature.md)

All decorators follow this general approach to describing functions in HGraph.
Note that from a signature perspective, the ``@graph`` decorator is the same as the differnt
node decorators. The intention is that a node can be converted into a graph and visa-versa
without any obvious changes to the use of function. This allows for easy refactoring of 
the implementation of a component, either breaking up complex nodes into smaller
parts and using a graph to wrap the parts together, or to take a graph and convert it
into a bespoke node for performance reasons.

Graphs
------

These functions are decorated with the ``@graph`` decorator.
The function is only ever called once when the graph is wired (prior to the evaluation
of the graph). The function can call other graphs or nodes. When `calling` the nodes you are
in reality only indicating a relationship between the nodes (or describing the edges in the graph).

It is best-practice to write the bulk of the logic in the form of graph functions.

Nodes
-----

Nodes are wired in graphs, but the node function is called whenever the time-series inputs
are ticked (the value changes over time). The node performs a computation and can emit
a result (if it is a ``@compute_node``).

Nodes are able to access additional state by expressing an interest in the state, this is further
described below:

[Node Signature Extension](node_signature.md).

The most common node type is the ``@compute_node``. This node takes in time-series inputs and emits
a time-series response.

a simple example is:

```python
from hgraph import compute_node, TS

@compute_node
def add(a: TS[float], b: TS[float]) -> TS[float]:
    return a.value + b.value
```

In this node we have a function called ``add`` that takes two time-series inputs (of type ``TS[float]``) and emits a 
time-series result of ``TS[float]``. Now, whenever a or b are ticked, the node will be called and the result will be
passed to the receiver of the result (with a time of the current clock's ``evaluation_time``).


