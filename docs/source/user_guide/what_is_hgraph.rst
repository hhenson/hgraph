What Is HGraph?
===============

HGraph is a framework for writing functional reactive programs. Programs are modeled as forward propagation graphs over time-series values.

The core model is:

- nodes perform computation, manage data sources, or produce side effects,
- edges connect node outputs to inputs,
- time-series values carry both data and time-oriented state,
- graph execution proceeds in ordered evaluation steps,
- changes propagate forward through dependent nodes.

This model is useful for real-time processing, simulation, backtesting, and other domains where time ordering and graph dependencies must be explicit.

Two Authoring Languages, One Runtime
------------------------------------

HGraph 0.8 is a C++ runtime with two supported authoring surfaces:

Python
    The ``hgraph`` package provides the DSL: the ``@graph``, ``@compute_node``
    and ``@sink_node`` decorators, the ``TS``/``TSL``/``TSB``/``TSD``/``TSS``
    type constructors, the operator library, services, adaptors, and the
    ``eval_node`` test harness. Python-authored nodes execute *inside* the C++
    runtime; Python does not implement a second graph engine. Start at
    :doc:`../getting_started`.

C++
    Graphs and nodes can be authored, wired, tested, and executed entirely in
    C++ against the native API, with no Python involved in the build or the
    run. Start at :doc:`cpp/quick_start`.

The two surfaces describe the same graphs and obey the same evaluation
semantics, so :doc:`concepts/index` applies to both. Where the surfaces differ
in what they expose, the difference is recorded in
:doc:`python_compatibility`.

Implementation Direction
------------------------

The C++ runtime is the source of truth. Python is a wiring and compatibility
surface, not the foundation. Concretely:

- system nodes are C++ only,
- C++ graphs and C++ nodes are first-class,
- Python graph wiring is fully supported and lowers into the same runtime
  construction data the C++ path produces,
- Python user nodes run inside the C++ runtime,
- the same runtime semantics apply across C++ and Python-authored graphs.

Version 0.8.0 replaced the Python-first implementation, which remains on the
``release/0.5`` maintenance line. Accepted behavioural deviations from that
implementation are recorded in the developer guide's parity matrix.
