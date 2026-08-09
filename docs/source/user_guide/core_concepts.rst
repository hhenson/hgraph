Core Concepts
=============

Forward Propagation Graphs
--------------------------

HGraph uses a forward propagation graph. Information flows from source nodes through compute nodes to sink nodes. When an input value changes, the runtime schedules dependent nodes and evaluates them in an order that preserves dependency correctness.

This differs from backward propagation systems, where a result is requested and dependencies are evaluated on demand. HGraph is designed for event streams and time-series processing, so changes are propagated as they occur.

Nodes
-----

A node is the unit of computation. Nodes have inputs and outputs and are evaluated by the runtime. Typical roles include:

- source nodes, which introduce data into a graph,
- compute nodes, which transform inputs into outputs,
- sink nodes, which perform side effects or expose final values,
- system nodes, which provide runtime behavior such as switching, mapping, reducing, and feedback.

In this C++ implementation, system nodes should be implemented natively in C++.

Graphs
------

A graph is a composition of nodes and edges. Graph construction is separate from graph execution. Wiring builds the graph structure, while the runtime owns evaluation, scheduling, state, and lifecycle.

Time-Series Values
------------------

A time-series value combines a scalar value with time-oriented state. The exact type model will be implemented in C++, but the stable concepts from the existing HGraph model remain:

- whether a value is valid,
- whether it was modified in the current evaluation step,
- when it was last modified,
- how it participates in graph propagation.

Evaluation
----------

Evaluation happens in time order. The runtime must preserve ordering across real-time and simulation modes so users can reason about causality and avoid accidental look-ahead behavior.

Wiring
------

Wiring is the graph construction phase. It resolves node signatures, graph boundaries, time-series types, and connections. In this project wiring is native C++ (see :doc:`authoring_graphs_cpp`); the planned Python wiring bridge must lower into the same runtime construction data the C++ path produces, so the runtime never depends on Python wiring.
