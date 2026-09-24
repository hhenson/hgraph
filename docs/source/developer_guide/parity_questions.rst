Open parity questions
=====================

Questions raised while triaging parity issues that neither the runtime
specification nor an earlier ruling decides. Each names the issue, the
observations, the options and a recommendation; the owner's answer moves it
into :doc:`parity_matrix` (accepted) or into a fix.

A native ``table_schema`` (issue #821, review of PR #1638)
-----------------------------------------------------------

Released hgraph 0.5.41's ``table_schema(tp)`` is a constant
``TS[TableSchema]``. ``TableSchema`` is released hgraph's compound scalar:
``tp`` holds the time-series type and ``types`` holds each column's Python
type (``int``, ``datetime``, …). The layout itself is computed in C++
(``table_ts_detail::ts_table_layout``, a supported public header).

Here, ``table_schema(tp)`` is an eager wiring-time helper: it assembles the
value in Python from that layout and returns it as ``.value``. It is not a
port, so it cannot be wired as a ``TS[TableSchema]`` (#821). PR #1638 adds
the constant time-series form inside a graph: it wires the same value with
``const`` and keeps ``.value``. This question is about what comes after that:
whether the operator should be native.

``AGENTS.md`` asks that a feature exposed to Python have an equivalent
first-class C++ wiring path. The design record *Record/replay, tables and
const_fn* (P1) proposed a native const-evaluable ``table_schema``. The
review on #1638 asked for one. It cannot be built without a type-system
decision:

- A native ``TableSchema`` needs ``tp`` and ``types`` as values. The only
  native carrier of a type is ``TypeCarrier`` (RFC 0033). It is a wiring-time
  argument and has no runtime Python conversion.
- Python's ``type`` annotation maps to the Python-object scalar on purpose: a
  user's ``type`` field may hold any class, including classes hgraph never
  registers. It cannot be redirected to ``TypeCarrier``.
- A native ``TableSchema`` without those fields, or with type *names*, is a
  second ``TableSchema`` beside released hgraph's (guardrail iii), and its
  ``types`` would differ from released hgraph's.

Options:

A. Keep ``TableSchema`` Python-owned, as #1638 does. C++ authors read the
   layout through ``TableLayout`` (keys, column schemas, partition and
   removed keys) at wiring, and ``to_table`` / ``from_table`` are native.
   Record the exception in ``parity_matrix.rst``.
B. Make types runtime values: a ``TypeCarrier`` Python conversion, a Python
   annotation for "an hgraph type" distinct from ``type``, then a native
   ``TableSchema`` bound to Python's by name (``namespace="hgraph"``). This
   is new public type-system API, so it needs an RFC.
C. A native ``TableSchema`` with type names, from which Python derives its
   own. Two schemas, and a native ``types`` unlike released hgraph's.

Recommendation: **A** now. Take up **B** through an RFC if C++ graphs need
the schema as a time-series rather than the layout at wiring.
