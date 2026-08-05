Python parity matrix
====================

The Python-to-C++ inventory (roadmap Priority 3's first deliverable): what the
Python ``hgraph`` surface (the ``ext/main`` reference tree) offers, what the
C++ runtime provides today, and precisely what is missing — so "done" for the
Python bridge is measurable rather than discovered.

**Snapshot: 2026-07-21.** Regenerate the operator section by scanning
``ext/main/hgraph/_operators/*.py`` for public ``def`` names and comparing
against ``lib/std``'s ``Operator<"name">`` markers and ``register_*``
call sites (three states below). Update this page in the same change as any
operator addition — a stale matrix is a doc/code-divergence bug.

Operator states:

- **Registered** — resolvable by NAME through ``OperatorRegistry`` (the
  Python-bridge contract, proven template-free by
  ``tests/cpp/test_erased_wiring.cpp``). Reachable from Python on day one.
- *Declared-only* — an ``Operator<"name">`` marker exists in the catalogue but
  no implementation is registered. Invisible to name resolution (exactly the
  gap class the erased-wiring suite caught for ``record``/``replay``).
- **Missing** — no C++ counterpart at all.

Parity acceptance rule
----------------------

A feature exposed through the Python package is complete only when it has an
equivalent public C++ wiring surface and native C++ behavioural tests covering
the same contracts.  The Python bridge may adapt Python syntax and values, but
must delegate runtime behaviour to the C++ implementation wherever the value
has a native representation.  A Python-only implementation is therefore a
compatibility stopgap, not parity.  Bridge-specific behaviour with no C++
meaning (for example dispatch over arbitrary Python object classes) must be
identified explicitly and must not become runtime infrastructure.

Operator catalogue
------------------

Of the **165** public operator definitions in ``hgraph/_operators``:
**136 registered**, **0 declared-only**, **0 missing** — **29** further names
are covered by equivalent C++/bridge APIs (catalogue rechecked 2026-07-21
after the release-hardening audit; the counts come from comparing
``operator_names()`` and the catalogue markers against the upstream scan).

.. list-table::
   :header-rows: 1
   :widths: 28 8 8 8 48

   * - Python module (``hgraph/_operators``)
     - Reg.
     - Decl.
     - Miss.
     - Gaps (declared-only *italic*, missing **bold**, equiv-API plain)
   * - Analytical (``analytical_operators``)
     - 4
     - 0
     - 0
     - equiv-API: center_of_mass_to_alpha, span_to_alpha
   * - Apply / call (``apply``)
     - 2
     - 0
     - 0
     - —
   * - Date & time (``date_operators``)
     - 4
     - 0
     - 0
     - —
   * - ``debug_tools``
     - 1
     - 0
     - 0
     - —
   * - Dedup (``dedup``)
     - 0
     - 0
     - 0
     - equiv-API: dedup_builder
   * - Flow control (``flow_control``)
     - 9
     - 0
     - 0
     - —
   * - Graph (``graph_operators``)
     - 7
     - 0
     - 0
     - equiv-API: pass_through_node
   * - ``json``
     - 2
     - 0
     - 0
     - —
   * - ``lift_operators``
     - 1
     - 0
     - 0
     - —
   * - Core operators (``operators``)
     - 49
     - 0
     - 0
     - equiv-API: accumulate, average
   * - Record / replay (``record_replay``)
     - 6
     - 0
     - 0
     - equiv-API: set_record_replay_model, record_replay_model, has_recordable_id_trait, get_fq_recordable_id, set_parent_recordable_id, record_replay_model_restriction
   * - Stream (``stream``)
     - 17
     - 0
     - 0
     - equiv-API: filter_by
   * - ``string``
     - 7
     - 0
     - 0
     - —
   * - Throttle (``throttle``)
     - 0
     - 0
     - 0
     - equiv-API: collect_builder
   * - Conversion (``time_series_conversion``)
     - 5
     - 0
     - 0
     - —
   * - ``time_series_properties``
     - 6
     - 0
     - 0
     - —
   * - JSON (``to_json``)
     - 2
     - 0
     - 0
     - equiv-API: to_json_builder, from_json_builder. **Recorded divergence:**
       RFC 0002 version-2 writers emit RFC 3339 UTC instants and canonical
       signed-microsecond durations instead of upstream's legacy text.
       Schema-directed readers accept both formats and normalize legacy input
       before any subsequent write.
   * - Table (``to_table``)
     - 3
     - 0
     - 0
     - equiv-API: table_shape, table_shape_from_schema, shape_of_table_type, set_as_of, get_as_of, get_table_schema_date_key, get_table_schema_as_of_key, set_table_schema_as_of_key, set_table_schema_date_key, make_table_schema, table_schema
   * - ``tsd_and_mapping``
     - 9
     - 0
     - 0
     - —
   * - TSS (``tss_operators``)
     - 0
     - 0
     - 0
     - equiv-API: compute_set_delta
   * - Type ops (``type_operators``)
     - 2
     - 0
     - 0
     - equiv-API: ``cast_``

Notes on the residue:

- ``dedup_builder`` / ``collect_builder`` are Python implementation-injection
  hooks, not user operators — the C++ overload registry covers the need
  natively. ``apply`` and ``call`` consume the native ``ValueCallable`` scalar;
  Python callables are bridge backends for that C++ value rather than a second
  operator implementation. The ``table_shape`` helper trio projects the
  native ``table_schema`` layout at Python wiring time.
- ``downcast_`` performs checked narrowing from a graph-realized closed
  CompoundScalar Bundle union to a derived ``TS``. ``downcast_ref`` performs
  the corresponding unchecked reference narrowing in C++; the Python helper
  only adapts the ``downcast_ref(Type, ts)`` syntax.
- The equiv-API names live in ``hgraph._table`` (bitemporal config +
  ``TableSchema``), the record/replay config/traits shims, the JSON builders,
  and small python helpers (``accumulate``/``average``,
  ``center_of_mass_to_alpha``/``span_to_alpha``, ``filter_by``, ``cast_``,
  ``compute_set_delta``).

Ported operator-test suite (the behaviour yardstick)
----------------------------------------------------

**All 48** upstream ``hgraph_unit_tests/_operators`` files are ported into
``python/tests/ported`` (the ctest gate ``hgraph_python_ported_suite``).
No implementation-gap skips remain in this tier. The two final stale markers
were removed on 2026-07-21 after the unchanged upstream cases passed: proxy
lag preserves sparse TSB field deltas, and every parameterized
``combine_status_messages`` case executes through the current stream schema
adapter. The sparse TSB behavior also has a public C++ ``eval_node`` regression.
TSS rebind-to-empty removal semantics execute through the keyed structural-REF
implementation.

Standing residue is limited to recorded deviations:

- **Recorded deviations** — python wiring-node signature introspection,
  CompoundScalar string representation, and
  ``test_to_table_dispatch`` (upstream ``_impl`` internals; behaviour covered
  through the public surface). Mapped children now publish the final validity
  of projected EMPTY-REF terminals and the corresponding test executes.

Expanded upstream ``all``-suite audit (2026-08-05)
--------------------------------------------------

The exact upstream tier now includes adaptors, the Arrow combinator package,
debug tooling, and examples.  Its first broad run exposed two genuine Python
authoring gaps, both fixed without changing native execution: anonymous
``compound_scalar(...)`` schemas once again allow omitted fields and retain
``to_dict``/``from_dict``, and ``with_columns[RowSchema](...)`` lowers that
released shorthand to the native ``TS[Frame[RowSchema]]`` output constraint.
Against released hgraph 0.5.36, the resulting 1,870-test audit records 1,092
exact matches, 762 evidence-backed conversions, 16 reference-unverified
tests, and no review-required, confirmed-gap, or ambiguous outcomes.

The 2026-08-05 evidence-frontier refresh installs Pydantic in both audit
environments, where the unchanged public compound-scalar case passes on both
engines.  Four upstream logging tests that are explicitly XFAIL because of
full-suite capture pollution XPASS in isolated reference processes; their
candidate module imports a retired private TSL implementation, so the report
retains the suite and isolated outcomes and accepts the existing public Python
and native logger coverage.  The exact real-time scheduler test's 90--110 ms
upper window was also observed to fail intermittently in the reference engine.
Its converted counterpart retains ordered values, tagged rescheduling,
not-before bounds, and the graph end-time while allowing wall-clock jitter.
The remaining 16 cases have no executable reference oracle: optional C++
memory/debug tests, timezone cases, the upstream multi-client adaptor XFAIL,
and skipped nested-graph, Kafka, and inspector cases.  They remain unverified
rather than being classified as candidate gaps or accepted conversions.

The remaining broad-suite differences are conversions of already recorded
design decisions, not unimplemented user behaviour:

- dataframe adaptors consume Polars at the boundary but publish canonical
  PyArrow values; equivalent join, filter, group, column, source, and
  record/replay assertions use Arrow values in the local Python and C++ suites;
- the old Python runtime-object ``inspector`` function is replaced by the
  native ``Inspector`` lifecycle observer and owned inspection snapshots;
- the ``SimpleArrayReplaySource`` example and process-global output-key
  builder exercise retired test/runtime infrastructure.  Formal replay APIs
  and native fixed key construction replace them; graph-specific policy is
  not stored in an unrelated process global;
- private ``wire_graph``/``WiringNodeInstance`` builder layouts remain outside
  the compatibility contract while public graph wiring is covered directly;
  the private Python ``InputsKey`` cache is likewise replaced by the native
  structural interning key, which compares producer identity, paths, input
  shape, and native scalar values without invoking overloaded port equality;
- Kafka tests inject consumers through the public ``consumer_factory`` option,
  and websocket behavior is covered without the upstream module's broad
  optional-import guard;
- ``cleanup_on_error=False`` defers stop only while the raised exception owns
  the failed executor; releasing it performs mandatory final teardown; and
- Arrow stop-hook errors cross the native exception boundary, explicit nodes
  are never pruned merely because their output is unconsumed, and runtime
  graph state is accessed through the ``GlobalState`` injectable.

Public surface refresh (2026-08-05)
-----------------------------------

The public API probe against released hgraph 0.5.36 compares 54 package and
adaptor modules, including callable defaults, parameter names, class methods,
and optional-module importability.  The refresh has no unclassified findings.
It restored the upstream ``CompoundScalar.from_dict(d=...)`` keyword across
the root and Tornado schemas, the standard ``TimeSeriesSchema`` reflection
surface on unspecialised ``RunGraphOutput``, and the ``_global_state``
injectable spelling on ``publish_output``.

The remaining threaded-graph signature differences are compatible widenings
or representation boundaries: ``global_state`` and ``params`` may additionally
be omitted, ``None`` represents the upstream default-output sentinel, and
service implementations are passive registration tokens in the C++-first
model.  The upstream-only ``_state`` parameter on ``adaptor_executor`` is a
hidden generator injectable; executor ownership remains inside the adaptor's
lifecycle node.  These call paths are pinned by public Python adaptor tests,
while independent-engine concurrency and runtime reference identification are
also covered natively.

Compatibility audit: wiring and time-series tiers
-------------------------------------------------

The follow-on inventory was reviewed on 2026-07-15 against ``ext/main`` at
``4760fccadd5368b0482393e5acb0ceaac48518e9``.  Upstream has 32 ``_wiring``
test modules containing 244 tests; 24 modules are now represented under
``python/tests/ported/_wiring``.  The saved post-error-handling baseline had 10
unported modules containing 118 tests, plus two tests in the separate root
``test_wiring.py``. ``test_service.py`` and ``test_lift.py`` are now partially
represented; the 120 tests remain the audit inventory rather than a live
missing-test count. Several assert
private Python builder objects that deliberately do not exist in the C++-first
model.

.. list-table:: Unported or partially ported wiring inventory
   :header-rows: 1
   :widths: 24 8 68

   * - Upstream module
     - Tests
     - Disposition
   * - ``_test_const_fn.py``
     - 4
     - **Accepted design difference.** Ordinary C++ functions are the
       wiring-time form; registered const-evaluable operators provide the
       dual eager/wired behaviour.  Do not add a ``const_fn`` node class.
   * - ``test_adaptor.py``
     - 13
     - Native source/sink/duplex and service-adaptor machinery is covered.
       Python ``service_adaptor`` / ``service_adaptor_impl`` declarations,
       automatic registration, and explicit multi-interface implementation
       stubs now use that erased native machinery.  Native ``stop_engine`` and
       the call-scoped ``EvaluationEngineApi`` injectable are public in both
       languages. Public real-time lifecycle behaviour remains. Private
       ``nodes._service_utils`` helpers are not API targets.
   * - ``test_context.py``
     - 17
     - Same-wiring named/default/required, shadowing, graph consumption, and
       explicit override are covered.  **Compiled boundary import has landed**
       for ``switch_``/``dispatch_``/``map_``/``mesh_``/``nested_``/
       ``try_except_`` children in the C++ capture path, with public C++ and
       Python ``eval_node`` coverage. ``Context<>`` parameters on registered
       overload implementations use the same scope resolution and are covered
       through public C++ ``eval_node`` wiring.
   * - ``test_de_dupping_of_nodes.py`` + root ``test_wiring.py``
     - 4
     - **Private-internal tests.** Native interning, scalar identity, sink
       non-interning, resolution, and graph topology have direct C++ tests.
       Python ``GraphBuilder``/``WiringNodeInstance`` object layouts are not a
       compatibility contract.
   * - ``test_error_handling.py``
     - 10
     - Native and public Python ``NodeError``, ``exception_time_series``,
       ``TryExceptResult`` / ``TryExceptTsdMapResult``, and ``try_except``
       execution are covered. TSD ``map_`` now captures child errors as
       ``TSD[K, TS[NodeError]]`` with native and Python ``eval_node`` coverage.
       Traceback depth and optional input-value capture lower onto immutable
       native error-capture options; portable C++ stack traces remain additive.
   * - ``test_lift.py``
     - 4
     - Basic lift, explicit output override, and ``dedup_output`` are covered;
       the latter wires the native ``dedup`` operator. ``lower`` is an
       Arrow-native C++ execution path over ``from_data_frame`` /
       ``to_data_frame``; the Python facade accepts PyArrow and preserves a
       Polars boundary when its input is Polars. Old ``Hg*TypeMetaData``
       signature assertions remain private.
   * - ``test_map.py``
     - 32
     - Public TSD/TSL mapping, key inference, explicit keys, broadcast and
       variadic arguments, injectables, lifecycle, reference retargeting,
       nested maps, and overloads have native and Python coverage.  Six tests
       inspect the old private ``_build_map_wiring`` structures and are not
       portable. Keyed-map error capture is covered by the error-handling
       runtime and public ``eval_node`` tests.
       **Accepted filter correction:** when a keyed input reopens after being
       filtered, hg_cpp reconciles the complete currently valid state. It
       removes every formerly published key that is now absent or invalid and
       does not republish unchanged values. Released hgraph 0.5.34 instead
       republishes unchanged values and omits an invalidated key; its own test
       marks that omission as a feature needing reconsideration. The corrected
       behavior is pinned by ``test_filter_str_tsd`` in the ported Python and
       native map suites.
   * - ``test_mesh.py``
     - 7
     - Named and anonymous meshes, dependency ordering/cycles, on-demand
       instances, membership, removal, and value-typed keys are represented by
       the public C++ and Python suites.  Port selected Python cases only as
       regression coverage; no new runtime design is implied.
   * - ``test_nested_graph.py``
     - 4
     - **Accepted design difference.** Native ``nested_`` is the C++ authoring
       primitive and covers source, value, compute, sink, REF, and structural
       boundaries. Python higher-order constructs expose registered C++
       operators that own their nested children; a generic Python
       ``nested_graph(...)`` primitive is not part of the supported surface.
   * - ``test_reduce.py``
     - 16
     - Fixed TSL, dynamic TSD, ordered non-associative folds, nested
       composition, removals, retargeting, and teardown are covered.  Dynamic
       TSL associative/ordered folds and dynamic-TSD pass-through-combiner
       outputs are also covered; ``REMOVE_IF_EXISTS`` is exported as a
       distinct sentinel with upstream-exact strictness (``REMOVE`` raises on
       absent keys; 2026-07-17), and ``REMOVE`` is the canonical sentinel name
       (the former ``REMOVED`` spelling is gone).  ``MAX_DT`` / ``MAX_ET``,
       ``utc_now``, ``EvaluationClock`` (usable as the clock-injectable
       annotation), ``get_recorded_value``, ``get_context``, ``TSW_OUT``,
       ``equal_lambdas`` and ``is_feature_enabled`` are exported at the
       package root (2026-07-17 export batch); the stale ``_KNOWN_GAPS``
       machinery is removed (every entry had a live implementation).
   * - ``test_service.py``
     - 19
     - **Public call shapes landed.** ``NUMBER``/``NUMBER_2`` lower to
       constrained native scalar variables; generic implementations resolve
       from clients; positional paths, multiple request arguments, reply-less
       request/reply services, and late subscription to an existing value all
       execute through the erased C++ service runtime. Matching public C++
       tests cover constrained generics, erased specialization identity,
       reply-less requests, and sampled late subscriptions. Keyed-service
       transport for both subscription and reply-full request/reply is selected
       by native wiring: decoupled sink/source implementations are direct,
       self-coupled implementations defer only the request, and service/adaptor-
       dependent request/reply implementations retain full feedback.
       Service-dependent subscription defers its key but keeps its response
       direct, preserving released rapid-transition behavior and shared
       mapped/reference value identity. Compiled ``map_``
       and ``mesh_`` children import
       their outer service transport inputs through the nested boundary; any
       response feedback is owned by the outer implementation, not by the child
       consumer. Private Python service-builder layouts are not compatibility
       targets.

The 215 upstream ``ts_tests`` tests were also copied mechanically to a
temporary directory and run against the current bridge under Python 3.12.8.
``REMOVE_IF_EXISTS`` was changed only in that temporary copy to the current
``REMOVE`` spelling so that the TSD module could collect.  The diagnostic
result was **159 passed, 56 failed**:

.. list-table:: Direct upstream time-series diagnostic
   :header-rows: 1
   :widths: 16 12 12 60

   * - Kind
     - Passed
     - Failed
     - Failure concentration
   * - ``REF``
     - 18
     - 12 at the saved audit
     - The audit failures for direct ``eval_node`` replay into ``REF`` and
       Python-node ``value``/``delta_value`` conversion have since landed,
       including sampled rebinding to an already-valid target. Runtime
       retargeting and structural REF behaviour have stronger native/ported
       coverage. Borrowed ``.output`` access remains prohibited.
   * - ``TS``
     - 39
     - 4
     - Two unparameterized ``TS[tuple]`` results, private ``bound`` topology,
       and the optional ``_output.can_apply_result`` helper.
   * - ``TSB``
     - 21
     - 5
     - Unparameterized ``TS[dict]``/``TS[tuple]`` results and private
       peered/non-peered topology inspection.
   * - ``TSD``
     - 20
     - 13
     - Mostly unparameterized container result annotations and mutable-output
       root inspection (``modified``/``delta_value`` after child mutation),
       not keyed storage or removal semantics.
   * - ``TSL``
     - 24
     - 7
     - Unparameterized tuple results, private peering, and mutable fixed-list
       ``clear``/inspection.
   * - ``TSS``
     - 24
     - 10
     - Unparameterized container results, mutable-output delta inspection, and
       recording a friendly set delta as a plain ``frozenset`` rather than the
       public ``SetDelta``-compatible subclass.
   * - ``TSW``
     - 13
     - 5
     - Pre-minimum validity suppresses consumer evaluation instead of ticking
       ``False``; ``removed_value`` throws before an eviction instead of
       returning ``None``; one failure is an unparameterized tuple result.

This run is deliberately **diagnostic**, not an acceptance gate: the files
were not adapted to the public C++-first test harness and several failures are
annotation, recording-shape, or private-topology assumptions.  Retained cases
must be rewritten around public ``eval_node`` graphs and paired with equivalent
C++ wiring tests.

The error-result schemas, call syntax, and native trace settings have landed.
The standard numeric vocabulary and selected generic service cases have
landed. ``SetDelta`` recording shape, safe REF metadata, mutable output-view
conveniences, and the two TSW view differences now have public ``eval_node``
coverage. Generic Python ``nested_graph(...)`` syntax is deliberately excluded
as described above.

Bare container **inputs** are generic family patterns.  ``TS[tuple]``,
``TS[dict]``, and ``TS[frozenset]`` accept only their respective Python value
families and specialize at wiring from the connected concrete source (for
example to ``TS[tuple[int, ...]]``, ``TS[dict[str, int]]``, or
``TS[frozenset[int]]``).  They do not erase a resolved input to ``object``.

A bare container **output** has no input-side fact from which its element,
key, or value types can be resolved.  It is therefore an intentional wiring
error for now.  Authors must declare the concrete container type, or explicitly
return ``TS[object]`` when an opaque Python object is the desired contract.
This makes schema loss visible and leaves a future runtime-validated bare
output adapter as an evidence-driven compatibility addition.  Peering,
binding, and old Python wiring-object layouts remain private diagnostics rather
than public compatibility targets.


Types and scalars
-----------------

.. list-table::
   :header-rows: 1
   :widths: 22 18 60

   * - Python
     - C++ status
     - Notes
   * - ``TS``, ``TSS``, ``TSD``, ``TSB``, ``SIGNAL``
     - Full
     - TSB is structural (field lists + named wrapper) by design — no schema
       inheritance / ``from_scalar_schema``.
   * - ``TSL``
     - Full
     - Fixed and dynamic.
   * - ``Frame[Rows, Metadata]``
     - Full for typed value transport and metadata-aware table transforms
     - Metadata is encoded field by field in Arrow schema metadata: supported
       atomics as plain strings and nested fields as JSON. Sorting, filtering,
       and column projection preserve it; concat requires equality; join
       requires an explicit metadata policy. C++ and Python bridge round trips
       use the same table representation.
   * - Downstream native scalar registration
     - Full
     - Installed C++ and public Python hooks associate extension-owned scalar
       schemas with Python classes for annotation resolution, reflection, and
       value conversion. Registration is process-wide, idempotent for an
       identical pair, and rejects conflicts.
   * - ``REF``
     - Full
     - Wiring marker + runtime retargeting; executor-level tests
       (``test_ref_executor.cpp``, std REF operators).  The Python value is
       intentionally opaque and never exposes a borrowed ``.output`` pointer.
   * - ``TSW``
     - Partial
     - Tick- and duration-based windows execute end-to-end in Python and through
       public C++ ``eval_node`` wiring. Duration windows have registry/runtime
       schemas but **no exact duration-valued compile-time marker**; C++ wires
       them through ``to_window`` with a ``TimeDelta`` period.
   * - ``CONTEXT``
     - Full (divergent)
     - ``context::scope<"name">`` / ``Context<"name", S>`` /
       ``context::get`` — **name-based** where Python resolves by type
       (recorded divergence); compiled children and registered overloads import
       the resolved context through the native capture path.
   * - ``STATE`` / ``RECORDABLE_STATE``
     - Full / partial
     - RecordableState storage+eval works; graph traits + recordable-id
       resolution landed (step 2 of :doc:`record_replay_table`);
       ``component<G>`` recording + RECOVER seeding landed (step 5 + P7).
   * - Scalars: ``bool int float str date datetime timedelta``
     - Full
     - Plus C++ extras Python lacks (width-specific ints, CyclicBuffer,
       Queue, mutable slot-store containers).
   * - ``time``, ``bytes``
     - Full
     - Landed 2026-07-04 as distinct strong types with standard-vocabulary
       registration.
   * - Enums
     - Full
     - C++ ``enum`` scalars register directly.  Runtime-defined Python enums
       lower to nominal C++ enum schemas and convert back to the registered
       Python class.
   * - DataFrame / Series, numpy arrays, JSON scalar
     - Full
     - Value kinds; gate the serialization operator families. **Ruling
       (2026-07-04): the Frame/table specification maps onto Apache Arrow
       tables, not Polars** — Polars sits on Arrow, so Polars (and other
       Arrow-native frame libraries) remain reachable by zero-cost
       transformation. Arrow is a **formal dependency**; the table is a
       first-class type behind the ``Frame`` marker (schema-less legal;
       input schema = minimum columns; output schema = exact). Full design:
       :doc:`record_replay_table`. **Scope ruling (2026-07-17): polars is
       boundary-only** — polars frames convert on ingest via
       ``__arrow_c_stream__``; frame reads yield ``pyarrow.Table`` and
       consumers use ``pl.from_arrow``. The to-frame ``convert`` family, the
       DATA_FRAME record/replay model (``recordable_id`` +
       ``set_data_frame_overrides``), the data-source generators, join,
       structural filters, group/ungroup, sorting, concatenation, column
       replacement/projection, and Series-to-tuple conversion execute
       Arrow-native. Python expression filters are retained for the
       Python-owned ``pyarrow.compute.Expression`` scalar. Native shaped arrays
       retain all ``Size`` dimensions and back the complete public
       ``hgraph.numpy_`` catalogue plus the NumPy helpers exported from
       ``hgraph.nodes``. Fixed shapes use inline capacity with a stored logical
       extent; unbounded dimensions
       use compact list storage. **Recorded temporal divergence:** RFC 0002
       version-2 ``Instant`` fields use Arrow ``timestamp[us, "UTC"]`` rather
       than upstream's timezone-free timestamp. Legacy timezone-free fields
       remain readable only at a schema-declared Instant boundary and are
       normalized to the version-2 representation on output.
   * - Generics (``TypeVar``, ``AUTO_RESOLVE``, ``Type[...]``)
     - Re-architected
     - ``TsVar``/``ScalarVar``/``SizeVar`` + wiring ``ResolutionMap`` +
       runtime ``TypePattern``. ``AUTO_RESOLVE``/``Type[...]`` have no
       counterpart (mitigated: erased views carry their schema).

Wiring and node-authoring surface
---------------------------------

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Python
     - C++ status
     - Notes
   * - ``@graph`` / ``@compute_node`` / sources / sinks
     - Full
     - Graph-as-struct + static node structs; all five node kinds including
       push sources (Queue/Conflating).
   * - Operator overloading, named args, defaults, kwargs, variadics
     - Full
     - Python calling rules in ``OperatorRegistry::resolve``.
   * - ``map_`` / ``switch_`` / ``reduce`` / ``mesh_`` / ``try_except_``
     - Full
     - Full Python call shapes (``__keys__``, key detection,
       ``pass_through``/``no_key``…), all-sink switches, scalar-configured
       Python switch branches, outputless keyed maps, live dynamic-TSD reduce
       zeros, projectable fixed-composite reduce results, ordered fixed-TSL
       folds, and ordered contiguous-integer TSD/tuple reduction with a live
       seed. Arbitrary graph/node and sink functions map over grow-only dynamic
       TSLs through stable in-place child slots; compatible lifted scalar
       kernels retain their single-node fast path. Python mesh self-reference
       follows the reference API: ``mesh_(func)[key]``, ``mesh_("name")[key]``,
       and ``get_mesh(func_or_name)`` return a lazy ``MeshWiringPort``;
       ``mesh_ref`` remains a C++ implementation primitive rather than a
       public Python function. Explicit ``__keys__`` also supports key-only
       mesh functions.
       **Documented reduce deviation:** an omitted associative zero is never
       inferred. Empty input remains invalid and a singleton bypasses the
       combiner. A supplied zero is the empty result and the singleton's second
       operand, but is ignored for two or more live values; unset ``TSL`` slots
       do not participate. When the supplied zero is **not** the combiner's
       identity, released hgraph's results are capacity-history-dependent: its
       node tree re-binds freed and padding leaves to ``zero``, so with
       ``add_`` and ``zero=-3`` three live keys publish ``sum + zero``
       (capacity-4 tree, one padded leaf) and an emptied TSD publishes
       ``2*zero`` or ``4*zero`` depending on how many keys the tree previously
       held. Upstream's own suite only exercises identity zeros, so that
       behaviour is an implementation artifact, not a contract; hg_cpp instead
       applies zero the fixed number of times stated above, which coincides
       with upstream everywhere the zero is an identity. Accepted deviation —
       issue #44; pinned by ``python/tests/test_reduce_zero_semantics.py`` and
       ``tests/cpp/test_reduce.cpp`` (issue-44 recipe case), suppressed by
       fingerprint in ``tools/parity/known_divergences.json``.
       A second accepted reduce deviation concerns **phantom mapped keys**:
       hg_cpp reduces the currently-valid values and may publish an
       intermediate aggregate while another live keyed slot is still invalid;
       released hgraph waits for that slot. Subsequent aggregates agree. This
       is the capacity- and latency-independent contract documented in
       :doc:`nested_graphs`; issue #95 is pinned by the mapped-service Python
       and C++ regressions and the bounded ``valid-subset-reduce`` parity
       family.
       The related unreduced-map deviation is also intentional: when a
       mapped ``switch_`` flips to a request/reply branch whose response is
       still in flight, hg_cpp removes the transiently invalid child output
       and adds it again when the response arrives. Released hgraph publishes
       an empty delta and retains the stale value. Issues
       #105/#117/#119/#133/#145 are pinned by the mapped request/reply Python
       and C++ regressions and the bounded ``switch-flip-map-removal`` parity
       family. RFC 0014 additionally makes a self-coupled response observable
       one cycle earlier than released hgraph 0.5.34 by removing its redundant
       response-feedback boundary. Standalone service recipes and the reduced
       nested-map shape from issue #274 are bounded independently by
       ``request-reply-one-cycle-earlier`` and
       ``nested-request-reply-one-cycle-earlier``. Only the nested relation
       permits an unchanged structural prefix, and it additionally proves the
       request/reply-backed alpha branch is active at the removed cycle. The
       switch/map family admits the
       same single-cycle advance only when composed with its exact flip, and
       the complete issue-175 outer-input collision remains fingerprint-pinned.
   * - ``dispatch_``
     - Full for Bundle values
     - Native ``dispatch_cases`` / ``dispatch_case`` wiring builds a closed
       leaf-type selection plan, inserts checked branch downcasts, and executes
       branches through the two-slot ``switch_`` runtime.  Exact, transitive,
       multi-argument, restricted-argument, default, no-match, and ambiguous
       inheritance behaviour is covered across
       ``tests/cpp/test_dispatch.cpp`` and
       ``python/tests/test_dispatch_scalar.py``. CompoundScalar dispatch uses
       this native path. Arbitrary Python object-class dispatch remains a
       bridge-only compatibility path because those classes have no native
       Bundle schema.
   * - ``feedback``
     - Full
     - One-cycle delay, sink/source pair.
   * - ``delayed_binding``
     - Full for peered and fixed structural sources
     - Shared C++ wiring-core placeholder with typed C++ and Python facades.
       It resolves before ranking, adds no runtime node or delay, preserves
       fixed-collection and key-set projections, expands fixed ``TSL``/``TSB``
       structures into delayed leaves, and rejects cycles. A dynamic structural
       ``TSL`` must first be materialized as a peered output because its
       cardinality is not declared.
   * - Services (reference / subscription / request-reply)
     - Full at a graph boundary
     - Path-aware and generic multi-interface implementations, template and
       erased descriptors, multiple request arguments, reply-less requests,
       reference-counted subscriptions, Python-compatible request/reply
       feedback, and service calls inside compiled keyed children.
   * - ``@adaptor`` / service adaptors
     - Full (core and supported families)
     - Source/sink/duplex and per-client keyed service-adaptor exchange are
       available from C++ and Python.  Python service-adaptor interfaces have
       one time-series request and one time-series response; bundles carry
       multi-field protocols. Tornado HTTP/WebSocket/REST, catalogue, JSON,
       SQL, Delta Lake, Kafka, Perspective, dataframe, executor, and threaded
       graph families use that boundary. See :doc:`roadmap` for the remaining
       explicit advanced-feature restrictions.
   * - Contexts
     - Full (wiring and compiled children)
     - Named/default/required compatibility plus native context capture across
       compiled nested operators and registered-overload injection.
   * - ``@component``
     - Full (first pass)
     - ``stdlib::component<G>`` over the mode scope + record/replay
       — the full mode set (Record/Replay/ReplayOutput/Recover/Compare;
       step 5 + P7 of :doc:`record_replay_table`).
   * - Injectables: STATE, SCHEDULER, CLOCK, GlobalState, OUTPUT
     - Full
     -
   * - Injectables: LOGGER
     - Full
     - ``LoggerView`` (``runtime/logger.h``) — spdlog in
       ``SPDLOG_FMT_EXTERNAL`` mode against the project fmt (the ruling); a
       transparent stateless injectable borrowing the executor-owned run
       logger. Root and nested graphs cache the same borrowed pointer;
       per-tick logging is refcount-free and does not consult process state.
   * - Engine control / ``stop_engine``
     - Full
     - Native ``EngineControlView`` is a copyable borrowed projection over the
       root executor. ``stop_engine`` is a C++ sink; Python wiring delegates to
       that registered operator. ``EvaluationEngineApi`` is a guarded Python
       projection over the same view, not a second engine object.
   * - Injectables: node self
     - Full
     - C++ implementations receive a zero-storage borrowed ``NodeView``;
       Python ``NODE`` exposes a callback-scoped projection over the same
       native node, including nested node/graph identity and notification.
   * - Lifecycle observers, evaluation trace/profiling
     - Full
     - ``EvaluationTrace`` is a public C++ observer and backs Python
       ``GraphConfiguration(trace=...)`` / ``__trace__``.
       ``EvaluationProfiler`` provides owned aggregate snapshots and backs
       ``profile=True`` / the upstream profile-options dictionary. Custom
       Python observers adapt to the same native observer list through guarded,
       callback-scoped graph/node views; ``eval_node(__observers__=...)`` uses
       that path too. Run-wide traceback/value capture and cleanup policy are
       native executor options.
   * - Wiring observers and wiring trace
     - Full
     - Native ``WiringObserver`` events cover top-level graphs, nested graph
       compilation, nodes, and the operator registry. Records own diagnostic
       text and retain only interned schema handles. ``WiringTracer`` backs
       ``GraphConfiguration(trace_wiring=...)`` and
       ``eval_node(__trace_wiring__=...)``. Python may supply that bound native
       tracer, but observer implementations and event records remain C++-only.
   * - Graph recovery (start-from-state)
     - Missing
     - Note: restart of a stopped instance is out of contract by design;
       recovery means *fresh* instances seeded from recorded state.

Runtime
-------

Both evaluation modes (Simulation, RealTime) are at full parity — Python has
no additional modes. Error handling includes bounded native activation traces
and optional input values. Portable C++ stack-trace text remains shallower than
Python; see :doc:`error_handling`.

What blocks the bridge vs. what is additive
-------------------------------------------

**Bridge-blocking** (must exist before the Python surface is useful):
no foundational bridge blocker remains.  The erased wiring contract, stable
ABI package, Python user nodes, runtime-defined enums, and expected-output /
resolution-map paths are operational.  Broad engine replacement still depends
on the required authoring slices identified by the compatibility audit above.

**Additive** (each lands independently and becomes available to Python
through the registry): optional catalogue additions, the remaining
serialization value kinds, trace/profiling, Python engine-control syntax, and
richer error-result compatibility. Dynamic-TSL mesh is not included: both the
upstream Python contract and this runtime's authoritative :doc:`mesh` design
define mesh as a TSD-only operator.
