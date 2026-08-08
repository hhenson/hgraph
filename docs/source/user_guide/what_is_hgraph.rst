What Is HGraph?
===============

HGraph is a framework for writing functional reactive programs. Programs are modeled as forward propagation graphs over time-series values.

In the original Python implementation, users describe graphs through a Python DSL. This C++-first implementation makes the native C++ runtime the source of truth: graphs are authored, wired, tested, and executed entirely in C++ (see :doc:`authoring_graphs_cpp`). A Python bridge onto the same runtime is planned for ecosystem compatibility, but Python is a compatibility surface, not the foundation.

The core model is:

- nodes perform computation, manage data sources, or produce side effects,
- edges connect node outputs to inputs,
- time-series values carry both data and time-oriented state,
- graph execution proceeds in ordered evaluation steps,
- changes propagate forward through dependent nodes.

This model is useful for real-time processing, simulation, backtesting, and other domains where time ordering and graph dependencies must be explicit.

Implementation Direction
------------------------

The objective of this repository is not to embed a C++ runtime behind a Python-first system. The objective is to make the C++ runtime authoritative, with Python wiring and Python user nodes supported as integration layers.

That means:

- system nodes are C++ only,
- C++ graphs and C++ nodes are first-class,
- Python graph wiring remains supported,
- Python user nodes can run inside the C++ runtime where needed,
- the same runtime semantics should apply across C++ and Python-authored graphs.
