Concepts
========

HGraph embodies a number of interesting and potentially unique ideas. In its
simplest form HGraph is an advanced event processor. This section describes the
key concepts and patterns that make up the framework.

These pages are language-neutral: they describe the model the runtime
implements, which is the same whether a graph is authored in Python or in C++.
The authoring guides (:doc:`../python/index`, :doc:`../cpp/index`) show the
spelling; these pages explain the meaning.

The shortest possible summary
-----------------------------

**Forward propagation.** Information flows from source nodes through compute
nodes to sink nodes. When an input value changes, the runtime schedules
dependent nodes and evaluates them in an order that preserves dependency
correctness. This is the opposite of a backward-propagation system, where a
result is requested and its dependencies are evaluated on demand. HGraph is
built for event streams, so changes are propagated as they occur.

**Nodes.** A node is the unit of computation. Source nodes introduce data,
compute nodes transform it, sink nodes perform side effects, and system nodes
provide runtime behaviour such as switching, mapping, reducing and feedback.
System nodes are implemented natively in C++.

**Graphs.** A graph is a composition of nodes and edges. Construction is
separate from execution: wiring builds the structure, and the runtime owns
evaluation, scheduling, state and lifecycle.

**Time-series values.** A time-series value combines a value with time-oriented
state — whether it is valid, whether it was modified in the current evaluation
step, when it was last modified, and how it participates in propagation.

**Evaluation.** Evaluation happens in time order, and that ordering is
preserved across both real-time and simulation modes, so causality is
reasonable about and look-ahead cannot happen by accident.

Each of those gets a full page below.

.. toctree::
    :maxdepth: 1

    run_loop_concept
    node_based_computation
    typing_system
    time_series_types
    dynamic_graphs
    services
