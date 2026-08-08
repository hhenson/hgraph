Error handling
==============

This page is the **authoritative design record** for runtime error handling:
per-node exception capture, the ``NodeError`` value, ``exception_time_series``
(the per-node extractor) and ``try_except`` (wrapping a whole sub-graph). It
mirrors the Python reference (``ext/main/hgraph/_wiring/_exception_handling.py``,
``_types/_error_type.py``) and records the deliberate C++ adaptations.

The substrate this builds on (an error-output port already plumbed into the
node storage, the ``ErrorOutput`` graph-edge source kind, and the
``single_nested_graph_node`` policy-wrapper view) is described in
:doc:`nested_graphs`.


Model
-----

A node evaluation can throw. The framework's contract:

- A node that **captures errors** runs its evaluation under a try/catch. On an
  exception it tries to build a ``NodeError``, writes it to its **error output**
  (a ``TS[NodeError]``) for that cycle, and the graph **keeps running**. This is
  best-effort stability, not a transaction: if the node wrote ordinary output
  before throwing, that output is unspecified and may remain observable. A node
  that does not capture errors lets the exception propagate (up to a
  ``try_except_`` boundary, else out of the graph).
- ``exception_time_series(port)`` is the **per-node** extractor: it activates
  error capture on the producing node and returns its error-output time series.
  For a TSD ``map_`` this is a sparse ``TSD[K, TS[NodeError]]`` with one
  retained error series per live mapped child.
- ``try_except(func, …)`` wraps a **whole sub-graph** in one node that runs the
  child graph under try/catch and produces ``TSB[{exception, out}]`` — the
  ``out`` field forwards the wrapped graph's output, the ``exception`` field
  ticks a ``NodeError`` when the child raises.

This matches Python: ``exception_time_series`` is the light per-node path and
``try_except`` the graph path. C++ provides both the statically typed
``try_except_<G>`` entry point and the erased ``wire<stdlib::try_except>``
operator; Python's public ``try_except`` uses the same erased operator. Keyed
``map_`` calls use their native per-child error output so errors retain keys;
Python compute nodes likewise use their native scalar error output to avoid a
nested-graph allocation.

An exception that neither path catches — one that escapes the **root** graph —
is annotated at the root evaluation boundary with the throwing node's identity
(``node[<index> '<name>'] evaluate failed: …``) so an unhandled failure is
diagnosable. This annotation never alters what capture or ``try_except_`` see:
they sit *below* the root boundary and receive the original exception, so
``NodeError.error_msg`` stays the raw ``what()`` (see *Runtime Diagnostics* in
:doc:`architecture`).


``NodeError``
-------------

``NodeError`` is a value-layer **compound scalar** (Python's
``CompoundScalar``) — a named ``bundle`` registered as ``"NodeError"`` with the
same fields as the reference:

==========================  =======  ============================================
field                       type     content
==========================  =======  ============================================
``signature_name``          ``str``  the node's display/signature name
``label``                   ``str``  the node label (empty if unset)
``wiring_path``             ``str``  best-effort path to the node in the graph
``error_msg``               ``str``  ``exception.what()``
``stack_trace``             ``str``  best-effort (see adaptation below)
``activation_back_trace``   ``str``  best-effort (see adaptation below)
``additional_context``      ``str``  optional context; unset by default
==========================  =======  ============================================

It is a first-class scalar type (``scalar_descriptor<NodeError>``), so
``TS<NodeError>`` resolves like any other time series and nodes may declare
``In<"err", TS<NodeError>>`` / ``Out<TS<NodeError>>``.

**Deliberate C++ adaptation — trace fidelity.** ``activation_back_trace`` is a
bounded native walk from the failed node through the inputs modified in that
cycle. ``trace_back_depth`` controls the number of upstream producer levels;
``capture_values`` additionally records current/delta values and last-modified
times. Value rendering is bounded and runs only after an exception. Python user
nodes appear by their C++ implementation node (for example ``__py_compute``)
and expose their packed native ``args`` input. ``stack_trace`` remains
best-effort because portable C++23 does not provide a usable stack trace on all
supported compilers; a Python exception's formatted traceback is retained in
``error_msg`` by nanobind. The field structure remains identical to Python.

For an exception that escapes the root graph,
``GraphExecutorBuilder::error_capture_options`` applies the same bounded native
walk before the runtime rethrows an annotated ``std::runtime_error``. Python
``GraphConfiguration(trace_back_depth=..., capture_values=...)`` configures
that executor policy directly; it is not a second Python traceback
implementation.

The executor also owns the cleanup decision. ``cleanup_on_error=True`` stops
the graph while propagating the exception. With ``False``, the Python exception
retains the failed executor so node stop hooks have not run while the exception
is being handled. Releasing the exception releases the executor and performs
the normal final stop; graph ownership and unsubscribe/destruction invariants
still apply.


Per-node capture and activation
-------------------------------

The node storage already reserves an error-output ``TSOutput`` when
``NodeTypeMetaData::error_output_schema`` is set, and the runtime ops serve it
(``error_output_view``); the missing pieces are capture and activation.

- **Capture (runtime).** ``evaluate_impl`` (``src/hgraph/runtime/node.cpp``)
  runs the user evaluate callback under a try/catch **only when**
  ``captures_errors`` is set. On ``std::exception`` it builds a ``NodeError``
  from the node identity + ``what()``; on a non-standard exception it builds one
  with ``error_msg = "unknown error"``. It writes the error to the error output
  for the cycle and returns normally. Ordinary output is deliberately
  unspecified on an error cycle because the framework does not roll back writes
  that happened before the throw. Scheduler re-arming still runs. A node without
  ``captures_errors`` pays nothing and propagates as before.
- **Diagnostic configuration.** ``ErrorCaptureOptions`` is immutable binding
  metadata. Repeated capture requests on one producer merge to the larger depth
  and enable value capture if any consumer requests it. The ordinary evaluation
  path does not walk inputs or stringify values.
- **Activation (wiring).** Node bindings are *interned* (schema + ops + storage
  plan are immutable and shared), so a node cannot be mutated in place to gain
  an error output. ``exception_time_series(port)`` therefore **re-binds** the
  producing node: ``NodeBuilder::with_error_capture(error_schema)`` clones the
  node's type record with ``error_output_schema = TS[NodeError]`` and
  ``captures_errors = true`` (reusing the original native callbacks; only
  supported for ordinary native nodes), and ``Wiring`` swaps the instance's
  builder before ``finish``. TSD ``map_`` is the custom-ops exception: it
  rebuilds its descriptor with a keyed error-output schema and catches each
  child evaluation independently. The later-built storage plan then includes
  the error output. The returned port is the node's error output, addressed by
  the existing ``ErrorOutput`` source kind.

Because activation happens during wiring (before ``finish``) the storage plan
is built from the amended schema. The error-output schema joins the interning
identity; in the rare case where a structurally identical node without an error
reference deduped to one that has it, the unused error port is harmless.


Keyed ``map_`` capture
----------------------

TSD ``map_`` owns a hidden ``TSD[K, TS[NodeError]]`` alongside its ordinary
output when capture is activated. A child exception updates only that key and
does not prevent other due children from evaluating. The error entry retains
its last value while the child remains live; logical key removal stops the
child and publishes the error-key removal, while slot erase performs the
existing in-place child destruction. Clean children never create empty error
entries or tick an empty dictionary.

The map error dictionary uses the same key schema as the ordinary map output
and ordinary ``TS[NodeError]`` elements. It is therefore planned as part of the
node's single allocation and follows the same stable key/slot lifetime rather
than maintaining a side table.


``try_except`` over a sub-graph
-------------------------------

``try_except_<G>(w, args…)`` compiles ``G`` into a child graph (the same
``compile_subgraph`` path as ``nested_<G>``) and adds one **try/except node**
built on the ``single_nested_graph_node`` substrate — the header advertises this
exact use ("policy wrappers such as try/catch … supply their own callbacks while
reusing the same storage and child-graph binding model").

- **Output shape.** The node output is a ``TSB`` with fields ``exception``
  (``TS[NodeError]``) and ``out`` (the wrapped graph's output) — Python's
  ``TryExceptResult``. A **sink** sub-graph (no output) yields just
  ``TS[NodeError]``. The ``exception`` field is owned by the wrapper; ``out``
  is a forwarding endpoint bound to the child terminal.
- **Catch.** The node's evaluate binds inputs, then runs
  ``child_graph().evaluate(t)`` under a try/catch. On a normal cycle the
  recorded ``NestedGraphOutputBinding`` keeps ``out`` forwarded to the child
  terminal; on an exception it moves a freshly built ``NodeError`` into
  ``exception`` instead. The child graph is **kept** across an exception
  (Python parity) and the graph continues.
- **Scheduling / lifecycle** delegate to the shared nested helpers
  (``single_nested_graph_bind_inputs`` / ``…_propagate_schedule``); start/stop
  reuse the substrate (custom start/evaluate skip the forwarding bind).
- **Interning.** Like ``nested_``: same ``G`` + equal inputs dedup; the child
  builder and its program-lifetime context are created on an intern miss.

The erased C++ form accepts any ``WiredFn`` and resolves positional/keyword
time-series arguments against that function before compiling the child:

.. code-block:: cpp

   auto result = wire<stdlib::try_except>(w, fn<RiskyGraph>(), x)
                     .as<TryIntResult>();
   auto detailed = wire<stdlib::try_except>(
                       w, fn<RiskyGraph>(), x,
                       arg<"__trace_back_depth__">(Int{2}),
                       arg<"__capture_values__">(Bool{true}))
                       .as<TryIntResult>();

Python exposes the corresponding public result schemas and call:

.. code-block:: python

   @graph
   def protected(x: TS[int]) -> TSB[TryExceptResult[TS[int]]]:
       return try_except(risky_graph, x)

``TryExceptTsdMapResult[K, O]`` describes the keyed ``map_`` form. Its
``exception`` field is ``TSD[K, TS[NodeError]]`` and follows mapped-child
lifetime.


Files
-----

- ``include/hgraph/runtime/node_error.{h}`` / ``src/…/node_error.cpp`` —
  ``NodeError`` scalar type, its interned ``bundle`` meta + ``TS`` meta, and
  ``make_node_error_value(...)``.
- ``src/hgraph/runtime/node.cpp`` — capture in ``evaluate_impl``;
  ``NodeBuilder::with_error_capture``.
- ``include/hgraph/runtime/try_except_node.{h}`` / ``src/…/try_except_node.cpp``
  — the try/except node on the nested substrate.
- ``include/hgraph/types/subgraph_wiring.h`` — ``try_except_<G>`` wiring entry;
  ``include/hgraph/lib/std/operators/higher_order.h`` and its implementation —
  the erased ``WiredFn`` operator;
  ``exception_time_series`` lives beside the existing ``error_output(port)``
  helper in ``graph_wiring.h``.
- Tests: ``tests/cpp/test_error_handling.cpp``.


Roadmap / deferrals
-------------------

Done: public C++/Python ``NodeError`` and ``TryExcept*`` schemas, per-node
capture + ``exception_time_series``, typed and erased ``try_except`` over a
sub-graph (value + sink), public Python call syntax, bounded activation traces
with optional input values, and keyed TSD ``map_`` capture.

Deferred (recorded, not yet built):

- richer portable ``stack_trace`` content;
- error capture on the remaining custom-ops nodes (nested/switch/mesh) via
  their own evaluate paths;
- child-graph reset/rebuild policy after an exception (currently kept as-is).
