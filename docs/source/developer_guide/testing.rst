Testing
=======

Testing should scale with the runtime surface being introduced.

C++ Tests
---------

C++ tests live under ``tests/cpp`` (add new files to
``tests/cpp/CMakeLists.txt``). They should cover:

- public headers and exported CMake targets,
- memory ownership and teardown,
- schema resolution,
- time-series state transitions,
- scheduler ordering,
- node lifecycle behavior,
- graph execution behavior.

The Catch2 unit-test executable links ``registry_test_listener.cpp``. The
listener resets all process-wide registries/factories before and after each
test case. Because ``reset()`` clears the singleton's normal auto-seeded state,
the listener then re-seeds the standard scalar/time-series vocabulary before the
test body runs. Tests should normally use the default registry state instead of
calling ``stdlib::register_standard_types()`` themselves; use a private test-only
scalar type when a test needs to exercise unregistered-type behaviour.

.. note::

   The teardown ordering is load-bearing: pointer-keyed plan/context registries
   must be cleared *before* ``TypeRegistry::reset()`` frees the schemas they key
   on, or a later test can intern a stale pointer (this caused real memory
   corruption once). The ordered sequence is library-owned —
   ``reset_all_registries()`` in ``hgraph/types/registry_reset.h`` — and the
   listener only delegates to it. Any new pointer-keyed registry must be added
   **there**, never as a second teardown sequence.

The graph unit-testing toolkit (design record)
----------------------------------------------

This section is the design record for the ``eval_node`` harness and its
substrate (``include/hgraph/lib/testing/`` — ``eval_node.h``,
``record_replay.h``, ``check_output.h``). The user-facing reference, with
worked examples for every time-series kind, is
*User Guide > Testing Graphs in C++*; this section records *how it works* so
the toolkit can be maintained and extended.

**Shape.** ``eval_node<NodeT>(inputs…)`` wires and runs a real graph under the
ordinary executor: one erased ``replay`` source per time-series input, the node
under test, and one erased ``record`` sink on its output. Tests deal only in
per-cycle value sequences — one element per engine cycle, ``none`` meaning "no
tick this cycle".

**Buffers.** ``replay``/``record`` move data through a cycle-aligned
``List<Any>`` buffer stored in ``GlobalState`` (seeded at wiring via
``Wiring::global_state()`` / read and written at runtime through the
``GlobalStateView`` injectable). ``set_replay_values`` /
``get_recorded_values`` are the raw access points the harness uses.

**Type erasure.** ``replay`` and ``record`` are *single erased nodes*, not
per-schema templates: capture uses the runtime, type-erased ``capture_delta``
(dispatch on ``schema()->kind``) to rebuild a canonical delta ``Value`` from
the live view, and replay applies deltas with ``apply_delta``. Adding a new
time-series kind therefore extends ``capture_delta``/``apply_delta``, not the
testing library.

**Element mapping** (``ts_harness<S>::element``): for ``TS<T>`` the harness
element is ``T``; for ``SIGNAL`` it is ``bool``; for the collection kinds
(``TSS`` / ``TSL`` / ``TSD`` / ``TSB`` / ``TSW``) it is a canonical delta
``Value`` built with the recursive builders ``set_delta`` / ``list_delta`` /
``dict_delta`` / ``tsb_delta``. Expected outputs are written with the same
``values<T>(…)`` helper used for inputs and compared by ``CHECK_OUTPUT``,
which uses ``Value::equals`` (order-independent for sets/maps) for erased
elements and ``==`` otherwise.

**Overloads.** Node forms cover sources (no time-series inputs; scalar arguments
follow directly) and input-driven nodes (input sequences first, then scalars;
multiple TS inputs, named arguments via ``arg<"name">(v)``, and node
``defaults()`` are supported). The operator form ``eval_node<Op>(…)`` dispatches
through the ``OperatorRegistry`` at wiring time and returns type-erased
``vector<optional<Value>>``. Graph forms mirror the source and input-driven node
forms. Use a minimal graph with concrete ``Port`` parameters and return type when
the item under test is generic or returns an erased port: the graph fixes the
signature, while ``eval_node`` still owns replay, record, execution, and result
collection. Do not hand-wire that harness in a behavior test. Callable arguments
(for higher-order operators such as ``map_`` / ``switch_`` / ``reduce``) are
passed as the ``WiredFn`` scalar ``fn<X>()``.

**Sources are not scheduled by default.** A source node in a test graph
initiates via ``schedule_on_start = true`` (declarative), a
``SingleShotScheduler`` (lightweight one-shot in ``start``), or a full
``NodeScheduler``. This mirrors the runtime rule that the graph schedule table
is the only activation gate.

**Reuse rule.** Tests reuse ``lib/std`` operators and the ``replay``/``record``
substrate rather than defining duplicate test nodes; a bespoke node in a test
file should exist only to exercise a shape the toolkit cannot express.

Evaluation tracing
------------------

``hgraph/runtime/evaluation_trace.h`` provides the native
``EvaluationTrace`` lifecycle observer. Attach it before executor construction
so it observes the root and every nested graph through the executor's shared
observer list:

.. code-block:: cpp

   EvaluationTrace trace{EvaluationTraceOptions{.start = false, .stop = false}};
   GraphExecutorBuilder builder;
   builder.graph_builder(std::move(graph)).add_lifecycle_observer(&trace);
   auto executor = builder.make_executor();
   executor.view().run();

The observer must outlive the executor run. It renders graph lifecycle events,
node ``[IN]``/``[OUT]`` values, future schedules, and nested graph paths. A
substring ``filter`` can restrict the trace to matching graph or node paths;
an optional native output callback supports embedding and deterministic tests.

The Python ``GraphConfiguration(trace=...)``, ``run_graph(__trace__=...)``, and
``eval_node(__trace__=...)`` forms construct this same C++ observer. ``True``
uses defaults; a dictionary accepts ``filter``, ``start``, ``eval``, ``stop``,
``node``, and ``graph``. ``hgraph.test.EvaluationTrace`` is the bound native
class, including ``set_print_all_values`` and ``set_use_logger``.

Evaluation profiling
--------------------

``hgraph/runtime/evaluation_profiler.h`` provides the native aggregate
``EvaluationProfiler``. Register it exactly like ``EvaluationTrace`` and read
an owned ``EvaluationProfileSnapshot`` after or during the run. Snapshot paths
and labels do not borrow graph or node memory, so keyed nested graph erase is
safe. Copies of a profiler share the measurement state; this is how the Python
object remains readable while the run owns its observer copy.

The profiler uses a monotonic clock and caches graph/node identities during
start. Steady evaluation updates perform pointer lookup, timing, and aggregate
updates without rebuilding paths. The recent window is a pre-grown circular
vector, so it allocates only on its first sample and not while rotating.
Without a registered profiler the observer list is empty and evaluation does
not read a clock or call Python.

The canonical native overhead workloads are
``evaluation_profiler_disabled_cycle`` and
``evaluation_profiler_enabled_cycle`` in ``hgraph_type_erasure_perf``. Run
them with:

.. code-block:: bash

   HGRAPH_TYPE_ERASURE_PERF_FILTER=evaluation_profiler \
     cmake-build-cpp/tests/cpp/hgraph_type_erasure_perf

Both workloads must report zero steady-state allocations. Timing comparisons
are recorded on the controlled Linux host; macOS runs are useful development
evidence but not a release performance baseline.

Runtime inspection
------------------

``hgraph/runtime/graph_diagnostics.h`` provides the native ``GraphDiagnostics`` lifecycle
observer. Register it on ``GraphExecutorBuilder`` and retain the caller handle;
observer copies share their C++ collector state:

.. code-block:: cpp

   GraphDiagnostics diagnostics;
   GraphExecutorBuilder builder;
   builder.graph_builder(std::move(graph))
          .add_lifecycle_observer(&diagnostics);
   auto executor = builder.make_executor();
   executor.view().run();

   GraphDiagnosticsSnapshot snapshot = diagnostics.snapshot();

Snapshots own their strings, hierarchy, timings, and storage counters. With
``capture_values`` enabled they also own JSON renderings and immutable Arrow
``Frame`` handles used by the Python tabular value view. They remain valid
after nested slot erase and executor destruction. ``reset`` is a between-runs
operation and throws ``std::logic_error`` while an executor is active.

Storage inspection is a cold path through ``NodeView::storage_metrics`` and is
never called when no diagnostics collector is registered.

Python Compatibility Tests
--------------------------

Python tests live under ``python/tests`` and should be used where Python wiring
or Python user nodes cross into the C++ runtime.

The continuously evolving released-hgraph comparison is documented in
:doc:`parity_testing`.  It generates bounded, multi-tick recipes and promotes a
verified mismatch into the ordinary Python and native C++ regression layers;
it does not replace either acceptance suite.

Architecture ratchets
---------------------

``python/tests/test_architecture_ratchets.py`` pins the number of occurrences
of a small set of source patterns that the 2026-09-04 fix-series retrospective
(PRs #525, #555, #610, #636) identified as a rule applied at the wrong layer.
Each entry names the layer that owns the rule:

* a std operator dereferencing its own ``REF`` input, or Python wiring
  handling ``is_ref``/``dereferenced`` by hand, when binding inserts the
  from-REF adaptation and the type-pattern matcher binds the dereferenced
  schema;
* the runtime probing ``TSTypeKind::REF`` per tick, when a node's REF handling
  mode is fixed when the node is built;
* Python wiring choosing a type carrier by operator name, or keeping a shadow
  schema-to-Python-type dictionary, when the resolver and the registry own
  both (the dictionaries are gone since RFC 0033's PR C: the bridge's
  reverse-binding registry is the one schema-to-annotation authority, and
  the ratchet holds it at zero);
* a second or third ancestry walker beside ``TypeRegistry::value_is_a``;
* operators recomputing ``value_type_for_active_realization`` instead of
  reading the binding from their bound views;
* Python-object hashing in more than one translation unit, and
  ``HGRAPH_ENABLE_PYTHON_USER_NODES`` conditionals inside the type layer;
* ``thread_local`` in the runtime;
* a bare ``catch (...)`` outside ``util/scope.h`` and the three documented
  translation boundaries -- an exception boundary without a name (see
  ``architecture.rst``, "Named exception boundaries").

The test fails when a count moves in either direction. A rise is a new copy
of a rule that already has an owner: fix it at the owning layer, or record the
deliberate exception in the relevant developer-guide page and raise the
baseline in the same change. A fall is the intended outcome of a
consolidation: lower the baseline in the same change so the ratchet stays
tight. ``HGRAPH_RATCHET_REPORT=1`` prints the current table with a per-file
breakdown instead of asserting. The test reads the source tree and skips when
run against an installed wheel outside the repository.

Authoring-shape sweeps
----------------------

The differential parity harness (:doc:`parity_testing`) varies tick sequences
over fixed authoring shapes. The defects in the 2026-09-04 retrospective sat on
the axes it does not generate: how a signature is spelled, how a type
hierarchy is declared, and how a ``REF`` nests through a consumer. The
authoring-shape sweeps cover those axes in the ordinary Python suite, on every
pull request, with a **self-consistency oracle** rather than a released-hgraph
oracle: the sweep wires the same consumer two ways and requires identical
``eval_node`` traces. That also lets them cover C++-first-only shapes.

``python/tests/test_ref_consumer_sweep.py``
   The rule: a consumer that does not declare ``REF`` observes the
   dereferenced value, because binding inserts the from-REF adaptation and the
   type-pattern matcher binds the dereferenced schema. Axes: input shape
   (``TS`` scalar, ``CompoundScalar`` including derived leaves, tuple, ``TSD``
   with string and polymorphic compound keys, ``TSS``, fixed ``TSL``, ``TSB``)
   × REF-producing source (a ``REF``-typed node, a fixed ``TSL`` projection,
   ``TSD`` item lookup, a ``map_`` element, a ``switch_`` branch, a switch that
   flips from a value body to a REF body, ``if_``, ``default``) × consumer
   (every std operator that accepts the shape, field access, ``combine``,
   ``collect``, ``mesh_``, ``dispatch``, a Python compute node). The plain
   source is the oracle arm and is asserted on its own, so a consumer
   definition mistake cannot masquerade as a runtime defect. Its first run
   found #649 (``reduce`` and ``mesh_`` reject a REF-valued collection) and
   #650 (a REF-output ``switch_`` goes silent after any branch change),
   neither of which any existing test or parity recipe reached; both are
   fixed and the sweep's gap tables are empty. A source that genuinely
   re-points consumers to a different output (the value-then-REF switch
   flip) samples that output at the flip, which on a collection is a
   full-value tick by design; such a source sweeps only the shapes where
   the trace oracle holds.

``python/tests/test_type_carrier_sweep.py``
   A *type carrier* is a ``type[...]`` parameter however it is supplied: a
   bare subscript ``fn[X]``, a named one ``fn[VAR: X]``, an explicit keyword
   ``to=X``, a ``DEFAULT[X]`` or bare ``= X`` default, ``AUTO_RESOLVE``, a
   scalar argument carrying a TS type or a type variable, a collection type,
   or a ``Size[n]``. Axes: decorator kind (``compute_node``, ``graph``,
   ``@operator`` with node and graph overloads, a generic reference service,
   a generic adaptor and a generic service adaptor -- the adaptor stubs have
   their own subscript rules) × carrier source × consumer (the body reading the materialised value,
   ``requires=`` seeing it, ``resolvers=`` seeing the binding) × ordering ×
   negative cells, plus the reverse binding ``T → schema → T`` over the scalar
   lattice and the bare-subscript pinning order of each decorator kind. Its
   oracle is *pinned current behaviour*: the carrier rules live once per
   decorator kind in Python wiring today (seven subscript rules, three
   type-variable collectors; the two shadow schema dictionaries were the
   first to go, in PR C) and the
   type-carrier blueprint moves the matching into the C++ resolver in stages,
   so the sweep records every per-kind inconsistency (``blueprint risk N``
   comments) and every cell that raises, and each stage is verified against
   it; a pin that a stage changes on purpose changes in that stage's PR. The
   ``wiring-type-carrier-sites`` ratchet counts the Python-side binding
   helpers the stages retire.

Each sweep carries a ``KNOWN_GAPS`` table of products that fail today, marked
``xfail(strict=True)``: a fix must delete its entry in the same change, and a
regression turns the entry from an expected failure into a failing test. When
a new product is found in production, add it to the relevant sweep's axes
first and let the sweep reproduce it; the fix then lands with the gap entry
removed and the matching architecture ratchet lowered.

Commands
--------

.. code-block:: bash

   cmake -S . -B build
   cmake --build build -j
   ctest --test-dir build --output-on-failure
   ./build/tests/cpp/hgraph_unit_tests   # run the Catch2 suite directly

Sanitizer configurations: ``-DHGRAPH_ENABLE_ASAN=ON -DHGRAPH_ENABLE_UBSAN=ON``
(Clang/GCC; exclusive with TSAN).

Open Design Items
-----------------

- Decide how to run Python compatibility tests against locally built bindings.
- Add sanitizer and leak-checking CI profiles.
