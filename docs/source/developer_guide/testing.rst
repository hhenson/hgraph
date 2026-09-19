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

The Catch2 suite is assembled from per-domain object libraries
(``HGRAPH_TEST_OBJECT_LIBS`` in ``tests/cpp/CMakeLists.txt``): every domain
is linked into ``hgraph_unit_tests`` and also built as its own
``hgraph_unit_tests_<domain>`` executable. One domain is conditional:
``hgraph_python_test_objects`` (``test_python_user_nodes_conversion.cpp``)
exists only under ``HGRAPH_ENABLE_PYTHON_USER_NODES``, embeds an
interpreter and converts through the bridge with no ``_hgraph`` module
(*Python Integration > Standalone conversions*); the "Linux
python-user-nodes" job of ``native-cpp.yml`` is the leg that builds it.

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

Value capture also records *navigation targets* -- which node and output each
input binding or reference leads to -- one record per element, so a keyed
collection contributes one per key. A target reached twice within one captured
value is listed once. That rule belongs to one file-local owner,
``TargetRecorder``, which answers "already recorded?" from a hash of the records
taken so far. It is created once per captured value by the capture entry points
and handed down through the functions that gather targets, because they recurse
through each other and the rule has to span all of them. Comparing each new
record against every earlier one instead made a capture cost the square of the
key count, and capture runs after every evaluation of the node: 1.6 s per
capture at 32,000 keys against 40 ms, flat at about 1 us per key
(``hgraph_unit_tests '[diagnostics-scaling]'``).

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
* two schemas compared as a paired ``dereference`` when
  ``time_series_value_equivalent`` (``endpoint_schema.h``) owns
  reference-transparent equivalence (RFC 0036; the count is that owner);
* the runtime probing ``TSTypeKind::REF`` per tick, when a node's REF handling
  mode is fixed when the node is built (RFC 0036: a
  structural hop goes through ``TSOutputView::through_reference()`` and the
  shared-output capture reads the record its link wrote at bind). The one
  counted exception is ``BoundaryTransfer::Plan`` construction in
  ``runtime/distributed_boundary.cpp``: its exhaustive schema switch rejects
  REF while compiling a materialized transport codec at wiring time. It does
  not follow references or select REF behavior during capture/apply; input
  binding owns materialization. A companion ratchet test pins this occurrence
  to the constructor's rejection case, so the baseline of one does not allow
  a new per-tick probe;
* Python wiring choosing a type carrier by operator name, or keeping a shadow
  schema-to-Python-type dictionary, when the resolver and the registry own
  both (the dictionaries are gone since RFC 0033's PR C: the bridge's
  reverse-binding registry is the one schema-to-annotation authority, and
  the ratchet holds it at zero);
* a second or third ancestry walker beside ``TypeRegistry::value_is_a``;
* a std operator resolving ``value_type_for_active_realization`` at all
  (``writing_nodes.rst``, "Resolve once in ``start``, read per tick": the
  bindings are read off the bound output; the ratchet holds the library at
  zero, and the operator-family lock matrix in ``test_registry_snapshot.py``
  guards the per-tick path);
* Python-object hashing in more than one translation unit,
  ``HGRAPH_ENABLE_PYTHON_USER_NODES`` conditionals inside the type layer,
  and ``nanobind`` spelled inside the type layer (RFC 0035: the type layer
  names Python only through the opaque references of ``python_object.h``
  and the ``PythonOps`` provider; both counts fell to zero family by
  family and the ratchets hold them there);
* ``thread_local`` in the runtime;
* a bare ``catch (...)`` outside ``util/scope.h`` and the three documented
  translation boundaries -- an exception boundary without a name (see
  ``architecture.rst``, "Named exception boundaries");
* JSON on a serialization path (RFC 0040, guardrail (v) in ``CLAUDE.md``): the
  JSON codec's names under ``runtime/``, where the floor is
  ``graph_diagnostics.cpp`` rendering values for a person, and under the
  persistence, Fabric and Kafka extensions, where the floor is the named
  ``json`` store codec -- for a store that is *meant* to hold JSON, never a
  default -- the read-only version 1 checkpoint reader, and Fabric's
  notification codec. That last one is the rule's other half: the binary
  codecs are for internal communication and state storage, and what hgraph
  encodes onto Kafka, an external boundary, is JSON, Avro or protobuf. The rule
  was only ever spoken, and JSON reached the internal paths twice because of it.

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
   a generic adaptor and a generic service adaptor) × carrier source ×
   consumer (the body reading the materialised value, ``requires=`` seeing
   it, ``resolvers=`` seeing the binding) × ordering × negative cells, plus
   the reverse binding ``T → schema → T`` over the scalar lattice and the
   bare-subscript pinning order of each decorator kind. Its oracle is the
   RFC 0033 contract: the registry matches, defers, materialises and ranks
   every type argument (``TypeArg``), and Python has one subscript rule and
   one type-variable collector, so every decorator kind behaves alike and a
   per-kind difference is a bug the sweep catches. The cells the cutover
   changed on purpose are the RFC's compatibility table, flipped in the PR
   that landed them. The ``wiring-type-carrier-sites`` ratchet stays at zero:
   a Python-side binding helper is a second implementation of the matcher.

Each sweep carries a ``KNOWN_GAPS`` table of products that fail today, marked
``xfail(strict=True)``: a fix must delete its entry in the same change, and a
regression turns the entry from an expected failure into a failing test. When
a new product is found in production, add it to the relevant sweep's axes
first and let the sweep reproduce it; the fix then lands with the gap entry
removed and the matching architecture ratchet lowered.

Recovery campaign
-----------------

Recovery (RFC 0023, RFC 0039) has one promise -- *a run restarted at a completed
day is indistinguishable from one that was never interrupted* -- and far more
ways to break it than hand-written cases can hold: it depends on which owners
are stacked (``map_``, ``mesh_``, ``dmap_`` in process and across processes,
``reduce``, a ``spawn_`` pipeline), how deep, where the component sits, where
the cuts fall, and what the keys were doing on each side of a cut. The
campaign, ``tools/recovery``, generates those products from a seed and judges
every one the same way.

**A scenario** is a *chain* (``dmapp__map__total``: layers over a leaf), a
*placement* (the component outermost, or *inside* the worker, where an owner
stands in for it), a *host* (wired in the main graph, or a stage of a
``spawn_`` pipeline whose sink runs in another process), a *mode*, a generated
event stream and its cuts. Streams are presence-aware -- a removal names a key
that is there -- with quiet cycles, and keys that leave and return.

**Three modes**, because they promise different things:

``clean``
   Never interrupted. The oracle for the other two, and a check that the
   scenario is well formed: a run that produced nothing to compare is an error.
``snapshot``
   Restart from a component checkpoint at each cut. State comes back, so the
   restarted run must match the unbroken one **delta for delta**.
``recover``
   Restart in ``RECOVER`` mode at each cut: the component's recorded *inputs*
   are re-seeded as a tick at the start of the day. No state comes back, so it
   is generated for graphs whose output is a function of their current input,
   and the promise is about **values**: a consumer that starts with the day and
   folds what it is sent holds, after every cycle, what the unbroken run held.
   The re-seed is an extra tick by design, so the deltas differ, and an
   accumulating node restarts from its last input -- also by design.

**Three ways a green campaign can lie, each closed:**

* *Comparing nothing to nothing.* A sample of scenarios is run again with the
  same cuts and **no recovery**. Where state crosses a cut that has to differ.
  A mode in which no control differed is reported ``VACUOUS`` and fails.
* *Expected refusals rotting.* ``model.expected`` says which placements are
  refused and why (a component below a user ``map_`` is not reached by an
  image). A scenario that was expected to be refused and ran fails as loudly as
  the reverse, so lifting a limit cannot go unnoticed.
* *An accepted defect quietly fixed, or quietly spreading.* A known defect is
  a **family** -- the relation that makes it reachable -- not a list of
  recipes, which would cover those recipes only and leave the next seed to
  rediscover it. Members that fail are ``known``, reported and not a failure;
  a failure outside every family is. When a large family stops failing
  altogether the campaign reports ``RETIRED`` and fails, so the fix deletes the
  family. Two exist today. ``tsd-restored-slot-order`` is a defect: a restored
  keyed input iterates its keys in another order when there is a removal on
  each side of a cut. It is pinned also as a strict ``xfail`` with its minimal
  stream in
  ``extensions/persistence/python/tests/test_reduce_recovery_scenarios.py``.
  ``mesh-empty-input-no-tick`` is **not** a defect but the consequence of a
  ruling (:doc:`parity_matrix`, no change means no tick): a ``mesh_`` started
  over an empty key set emits nothing, one emptied later keeps its valid empty
  output, and ``RECOVER`` is a fresh start -- so where a ``mesh_`` layer's own
  input is empty at a cut, the enclosing key is absent afterwards.

**Where it runs.**

* Every test run: ``test_recovery_campaign.py`` runs every fifth scenario of
  the ``pr`` profile in a few seconds, so the machinery is never first
  exercised at night.
* Nightly, ``.github/workflows/recovery-nightly.yml``: candidate core and
  persistence wheels, the ``nightly`` profile over 16 shards with the run
  number as seed, then a **verdict** job. A shard is too small a sample to call
  a mode vacuous or a family retired, so shards never fail the run; ``merge``
  judges the totals (and that every shard reported) and needs no hgraph. A
  pull request that touches the campaign runs the ``pr`` profile through the
  same jobs.
* The same workflow runs the **save and restore benchmark**,
  ``tests/cpp/test_recovery_benchmark.cpp`` (hidden from the ordinary suite,
  ``[.][recovery-benchmark]``): ``map_`` as the bar, then ``dmap_`` as a
  component member and hosting a component, in process and across processes,
  at 5k / 10k / 20k / 40k keys. It differences a day with and without recovery
  configured, prints one JSON row per size, and **requires** the cost per key
  to stay flat (guardrail iv). Each row also splits the recovered days at the
  two moments recovery calls out -- ``load`` (the graph is built) and
  ``commit`` (it has been captured and stopped) -- into ``build``, ``run`` and
  ``destroy``, because a difference of medians cannot say which part grew. The
  image a day restores from comes from an untimed run, is staged before the
  clock starts and is handed over by move, as a store does; copies made inside
  the clock once inflated the restore figure by up to a half and changed which
  phase appeared to grow. Results: ``benchmarks/results/recovery-*``.

.. code-block:: bash

   python -m tools.recovery campaign --profile pr            # ~100 scenarios, ~30 s
   python -m tools.recovery campaign --profile nightly --shard-index 0 --shard-count 16
   python -m tools.recovery merge recovery-reports           # the verdict on a sharded run
   python -m tools.recovery replay recovery-results/report.json   # re-run what failed

A report's failures carry their full recipes, so ``replay`` needs nothing else.
The campaign must run against a **real install** (core wheel, then the
persistence extension built against its SDK): that is the only configuration
in which the two share one core.

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
