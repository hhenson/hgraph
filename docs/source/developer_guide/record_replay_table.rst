Record/replay, tables and ``const_fn`` — design record
=======================================================

.. note::
   **Status: APPROVED design (rulings 2026-07-04, recorded in *Rulings*
   below).** The page keeps the analysis of the Python machinery for the
   record, then states the approved C++ shape. Standing ruling folded in:
   the Frame/table specification maps onto **Apache Arrow**, not Polars —
   Polars sits on Arrow and stays reachable zero-copy.

How the Python machinery works
------------------------------

The pieces and their dependency order (each depends on those above it):

1. **``const_fn``** (``_wiring/_decorators.py``) — a function over scalar
   inputs producing a constant value, declared with a time-series return
   type. Its trick is **dual-mode calling**: inside a graph it wires as a
   const source; outside a graph it evaluates eagerly and returns a value
   (``my_const(1, 2).value``). It participates in operator overload
   resolution (``overloads=`` / ``requires=``), so a ``const_fn`` can be a
   backend-selected implementation of an operator.

2. **Table schema derivation** (``_to_table_dispatch_impl.py``) — the part
   worth keeping as-is in spirit. ``extract_table_schema`` is a recursive
   ``singledispatch`` over type metadata producing a ``PartialSchema``: the
   flattened column ``keys``/``types`` plus **composed closures**
   ``to_table`` / ``to_table_sample`` / ``to_table_snap`` / ``from_table``,
   assembled child-by-child and cached per type. A row is
   ``(last_modified_time, as_of, *values)``; TSD adds partition-key columns
   and a removed flag; three write modes (Tick / Sample / Snap).
   ``table_schema`` is exposed as a ``const_fn`` so wiring code can read the
   schema as a value.

3. **``to_table`` / ``from_table`` operators** (``_to_table_impl.py``) —
   thin nodes over the composed closures; the ``TABLE`` type is a nested
   Python tuple (``tuple[*row]`` or ``tuple[tuple[*row], ...]``).
   ``from_table_const`` is a ``const_fn``. The date/as-of **column names and
   the as-of time come from GlobalState** (``set_table_schema_date_key`` /
   ``set_as_of``).

4. **``to_json`` / ``from_json``** (``_to_json.py``) — the same composed
   converter-builder pattern (``to_json_builder(tp, delta)`` returns a
   cached closure pipeline), independent of tables; output is ``TS[str]``
   (the separate ``json`` *scalar* type is only needed by
   ``json_encode``/``json_decode``).

5. **Record/replay** (``_record_replay.py`` + backends) —

   - ``record(ts, key, recordable_id=None)`` / ``replay(key, tp)`` /
     ``replay_const(key, tp, tm, as_of)`` / ``compare(lhs, rhs)`` operators;
   - a **model registry**: the active backend is a *string in GlobalState*
     (``set_record_replay_model``), and each backend registers its operator
     overloads guarded by ``record_replay_model_restriction`` — a
     ``requires`` predicate that reads that global **at wiring time**;
   - **recordable ids** resolve through **graph traits**: a parent-chained
     key-value store on graphs (``get_fq_recordable_id`` walks
     ``parent.child`` chains);
   - ``RecordReplayContext`` — a wiring-time context-manager stack carrying
     ``(mode, recordable_id)``;
   - the **data-frame backend** (``adaptors/data_frame``) implements the
     operators on ``to_table``/``table_schema`` + Polars frames, keyed by
     recordable id, with bitemporal as-of filtering (this is the part the
     Arrow ruling re-targets);
   - ``replay_const`` **must be a const_fn**: RECOVER mode needs recorded
     state *as a wiring-time value* to seed graphs.

6. **``@component``** (``_component_node_class.py``) — wraps every input in
   ``input_wrapper`` and the output in ``output_wrapper``, which consult the
   ambient ``RecordReplayContext`` mode and conditionally wire
   ``merge(ts, replay_const(...))`` (RECOVER), ``replay`` (REPLAY/COMPARE),
   ``record`` (RECORD), ``compare`` (COMPARE). **The graph's shape is a
   function of the ambient mode.**

The design issues
-----------------

**I1 — ``const_fn`` as a concept.** Python needs it because wiring code is
Python and node bodies are Python: one function usable both as a plain call
and as a graph source requires a dedicated node class. In C++ the wiring
language *is* C++ — any ordinary function is already callable at wiring time,
and ``const_(fn(args...))`` turns its result into a source. What ``const_fn``
*additionally* provides is (a) overload participation and (b) a uniform shape
the Python bridge can evaluate eagerly.

**I2 — overload resolution coupled to mutable global state.** The backend
restriction predicate reads ``record_replay_model()`` from GlobalState during
wiring. Resolution outcomes therefore depend on ambient mutable state — this
collides with hg_cpp's interning model (node identity derives from explicit
arguments and resolved schemas) and makes wiring non-reproducible.

**I3 — mode-dependent graph shape.** ``@component``'s wrappers change the
wired topology per ambient mode. Conditional wiring is fine in itself
(compose code is code); the issue is that the *ambient* mode is invisible to
the call site and to interning identity.

**I4 — stringly-typed global configuration.** Date/as-of column names, the
as-of override, the model string, and replay seeds all live in GlobalState
under ``::magic::`` keys — set imperatively, read at wiring or eval time.

**I5 — the TABLE value.** Nested per-tick tuples are idiomatic Python and
wasteful C++: materialising a row value per tick, then accumulating rows into
a frame in the recorder, costs two copies and a type (``TABLE``) with no C++
identity. The Arrow ruling changes the natural target: columnar builders.

What maps cleanly (keep)
------------------------

- **The builder pattern is our ops-table pattern.** ``PartialSchema`` — a
  struct of composed per-type closures, cached per type — is precisely an
  interned per-schema **serializer ops table** (struct of fn-ptrs synthesized
  recursively over ``TSValueTypeMetaData``, interned like plans/ops). The
  part of the design Howard likes survives as the part hg_cpp already does
  everywhere.
- **Bitemporal rows** (``(value_time, as_of, *columns)``), partition keys +
  removed flags for TSD, and the Tick/Sample/Snap mode triad — sound
  semantics, keep as-is.
- **Fully-qualified recordable ids** via parent-chained traits — sound; needs
  a small graph-traits primitive.
- ``to_json``/``from_json`` are independent of tables and need only the
  serializer-ops synthesis + ``TS<Str>``; they can land first as the
  proving ground for the synthesis machinery.

Proposed C++ shape (for discussion)
-----------------------------------

**P1 — no ``const_fn`` node kind.** A wiring-time computation is a plain
function; a const source is ``const_``. For the two roles that remain:

- *Overload participation*: allow registering an operator implementation as
  **const-evaluable** — an ``OperatorImpl`` flag plus an eager kernel
  ``Value(std::span<const Value>)``. ``resolve`` works unchanged; a caller
  (C++ wiring code or the Python bridge) may invoke the eager kernel instead
  of wiring a node. ``table_schema`` / ``from_table_const`` /
  ``replay_const`` register this way.
- *Python-bridge parity*: the bridge exposes eager evaluation of
  const-evaluable operators — Python's dual-mode behaviour reproduced
  without a node class.

**P2 — configuration in graph GlobalState.** The record/replay model,
date/as-of keys, and as-of override are one typed **RecordReplayConfig** value
in the top-level graph's ``GlobalState``.  Operator resolution reads the LIVE
selected state during wiring (ruling 2026-07-27) — the same store Python's
imperative setters write, so configuration applied during wiring, including
inside a graph function, is honoured — while runtime nodes receive the root
graph's isolation copy, captured at graph build.  Configuration must not
change between graph build and execution.  There is no second process-global
store.

**P3 — modes via the context machinery, folded into identity.** The
``RecordReplayContext`` maps onto the landed wiring-scope machinery (a
dedicated stack beside the context stack on ``OperatorRegistry``), carrying
``(RecordReplayMode, recordable_id)``. The component/graph that consults it
must fold the consulted mode into its intern key (the ``MapCallConfig``
precedent) so identical calls under different modes are distinct instances.

**P4 — Arrow-native tables, ``Frame`` as a first-class type.** Apache Arrow
is a **formal dependency** of the project (fetched/required like fmt, not an
opt-in build flag). The Arrow table is a first-class scalar value kind,
**abstracted behind the ``Frame`` marker** (mirroring Python's
``Frame[Schema]``) so user code names ``Frame``/``Frame<Schema>``, never
Arrow directly. Schema semantics (ruling):

- ``Frame`` with **no schema** is acceptable (an untyped frame);
- a schema on an **input** is a *minimum requirement* — the arriving frame
  must contain **at least** those columns at those types (structural
  width-subtyping; extra columns pass through);
- a schema on an **output** is *exact* — the produced frame must have
  **exactly** that schema.

The typed value also supports ``Frame[Rows, Metadata]``. Frame-level metadata
is encoded field by field into the Arrow schema metadata, not stored in a
second native object and not repeated as Arrow columns. A reserved schema key
records the qualified hgraph Bundle type; supported atomic fields use plain
strings and structured fields use the native schema-directed JSON codec.
Direct ``FrameStore`` reads and writes therefore preserve metadata as part of
the Arrow representation. Serializing a stream whose *row payload* is itself a
metadata-bearing Frame into the generic bitemporal table layout is not yet
defined; a backend must preserve or explicitly transform those schema entries
when that case is added.

``TableSchema`` maps to an Arrow schema (``value_time``/``as_of`` as
timestamp columns; partition keys; removed as a bool column). The serializer
ops write **directly into Arrow array builders** (append per tick), so the
recorder accumulates a ``RecordBatch`` without a per-tick row value;
``from_table`` walks Arrow arrays. The user-visible ``to_table`` operator (a
``TS`` of row values) stays for parity, but record/replay backends bypass it
and drive the serializer ops directly — one copy, no intermediate tuples.

**P5 — graph traits primitive.** A small parent-chained key-value store on
graph storage (build-time write, runtime read), carrying ``recordable_id``
and later the rest of Python's traits surface. Prerequisite for
``@component``.

**P6 — a registered, type-erased content store.** ``compare`` (and related
record/replay code) reads/writes results through a **type-erased store
abstraction** — an ops-table interface (open/read/write/list over keyed
content) with implementations **registered** the same way operator backends
are. The in-memory GlobalState buffer is the default registration; file /
Arrow-dataset stores register alongside it. This replaces Python's "writes
to a comparison result file" with a pluggable seam shared by the recorder
backends.

**P7 — recovery seeds state directly.** Python's RECOVER wires
``merge(ts, replay_const(...))`` — an expedient, not a design (ruling).
The C++ recovery path takes the efficient route: recorded values are
written **directly into node outputs / recordable state during graph
start** (the seeding pass runs before the first evaluation cycle), with no
merge nodes added to the topology. ``replay_const`` remains available as a
const-evaluable operator for explicit wiring-time reads.

**Sequencing** (each lands green independently):

1. **DONE (2026-07-04)** — serializer-ops synthesis + ``to_json``/``from_json``.
2. **DONE (2026-07-04)** — graph traits + RecordReplayConfig + mode scope.
3. **DONE (2026-07-04, first pass)** — Arrow dependency + ``Frame`` value
   kind + ``to_table``/``from_table``.
4. **DONE (2026-07-04, first pass)** — the Arrow record/replay backend +
   ``replay_const`` reads (RECOVER seeding remains — see below).
5. **DONE (2026-07-04, first pass)** — ``component<G>``
   (Record / Replay / ReplayOutput; Compare + Recover throw pending their
   pieces).

Step 1 — landed
---------------

``types/value/json_codec.h`` — the interned per-schema ``JsonConverter``
(struct of write/read fn-ptrs + child converters, synthesized recursively
over ``ValueTypeMetaData`` and interned; cleared by ``reset_all_registries``
— the Python ``PartialSchema``/``to_json_builder`` pattern as a C++ ops
table). ``to_json_string(view)`` / ``from_json_string(meta, text)`` are the
value-layer entry points; parsing is meta-directed recursive descent (no
DOM). Operators ``to_json(ts, delta=false) -> TS<Str>`` and
``from_json -> OUT`` (output schema at the wiring site) are registered
(``operators/json.h`` + ``impl/json_impl.h``). The ``delta`` flag is a
wiring-time constant, so value vs delta is resolved by **overload selection**
(``requires_`` on the flag) — the per-tick evals are branch-free
(wiring-time resolution over run-time cost). Tests:
``tests/cpp/test_json.cpp``.

Wire-format decisions (vs Python, recorded):

- Value forms match Python exactly for atomics (dates/times/timedeltas in the
  strftime shapes), bundles/tuples, lists/sets, and maps (non-string keys
  rendered then quoted).
- **Delta mode serialises the canonical delta value** (``capture_delta``) —
  for ``TS`` scalars/compounds this equals the value form; for set/dict
  time-series the canonical delta shape is the wire form rather than
  Python's per-TS-kind ad-hoc delta JSON (reconcile at bridge time if the
  Python tests require byte-identical delta JSON).
- ``from_json`` applies the parsed value as the tick's delta — the Python
  support surface (``TS`` over scalars/compounds).
- Not yet serialisable (throw at converter synthesis): ``bytes`` (no Python
  JSON form either), enum names (needs the runtime enum name table),
  ``Any``/CyclicBuffer/Queue.

Step 2 — landed
---------------

``types/record_replay.h`` + graph traits (implements P2, P3, P5):

- **``record_replay::Config``** (P2) — the typed graph/run
  configuration: ``model`` (default ``IN_MEMORY``), the bitemporal
  ``date_key``/``as_of_key`` column names, and the optional ``as_of``
  override. ``set_config(GlobalStateView, Config)`` runs before wiring;
  backends guard overloads with ordinary ``requires_`` predicates via
  ``record_replay::model_is`` over the state carried by operator resolution.
  Python's imperative setters (``set_record_replay_model``,
  ``set_table_schema_date_key``, ``set_as_of``…) become bridge shims over
  the Python thread's seed.  Registry reset clears transient wiring scopes but
  does not own graph/run configuration.
- **``record_replay::scope``** (P3) — the RAII mode scope
  (``Mode`` flag set mirroring ``RecordReplayEnum``, plus a recordable id);
  ``current_scope()`` reads the innermost entry. Consumers that consult it
  while wiring must fold the consulted state into their intern identity
  (enforced by convention now; ``@component`` in step 5 is the first real
  consumer).
- **Graph traits** (P5) — ``GraphBuilder::trait`` / ``Wiring::set_trait``
  write; two read accessors mirroring Python's ``Traits`` exactly:
  ``GraphView::trait(name)`` is the **chained lookup that bubbles up the
  nested parent chain** (Python ``get_trait`` — children inherit and may
  shadow), while ``GraphView::trait_or(name)`` reads **this graph's own
  entry only** (Python ``get_trait_or``). Both return an invalid view when
  absent (the C++ absence idiom; Python's ``get_trait`` throws).
  ``fq_recordable_id`` resolves through the *chained* accessor — the
  bubbling replaces Python's explicit copy-down of fq ids onto child
  graphs, with identical results when each component level sets its fq
  trait and strictly more forgiving when one doesn't. The store is the
  **value-layer mutable ``Map<string, Any>``** — the same shape as
  ``GlobalState`` (one runtime model; no parallel std-container store).
  Setters take a borrowed ``ValueView`` (copy-assign in place) or an rvalue
  ``Value`` (moved into the ``Any`` box — ``MutableAnyView::set(Value&&)``
  and ``GlobalStateView::set(key, Value&&)`` were added for this and benefit
  every GlobalState writer). Storage rides the shared graph runtime header;
  the ops-table entry ``trait_impl`` returns the graph's own entry, the view
  does the walk. ``record_replay::fq_recordable_id(graph, local)``
  implements Python's ``get_fq_recordable_id`` over the trait chain
  (``RECORDABLE_ID_TRAIT = "recordable_id"``).

Tests: ``tests/cpp/test_record_replay_config.cpp`` (config + reset, scope
nesting/shadowing, root traits, nested chain + shadowing, fq-id rules).

Step 3 — landed (first pass)
----------------------------

Apache Arrow is wired as the **formal dependency** (``find_package(Arrow
CONFIG REQUIRED)``; a package-manager install is expected — Arrow is too
heavy to FetchContent by default). What landed:

- **``Frame``** (``types/frame.h``) — the first-class table scalar backed by
  ``std::shared_ptr<arrow::Table>`` (forward-declared; user code and operator
  headers never see Arrow). Registered in the standard vocabulary
  (``"frame"``, ``TS[frame]``). Copying copies the handle (Arrow tables are
  immutable); equality/hash are **handle identity** — cheap and honest for
  tick suppression; content comparison stays an explicit operation. The
  shared handle is an opaque third-party resource (like ``Str``'s heap
  buffer) — the no-shared-ptr rule governs runtime graph structure, not
  foreign value payloads.
- **``TableConverter``** (``types/value/table_codec.h``) — serializer ops
  interned by value schema and configured date/as-of names: flattened column
  layout (``[date_key, as_of_key, *columns]``) with per-column append/read
  fn-ptrs writing **directly into Arrow array builders** and reading from
  Arrow arrays — no per-tick row tuples. Leaf coverage: all standard atomics
  (bool/int/float/str/date/datetime/timedelta/time). v1 value coverage:
  atomics + depth-1 bundles; TSD partition keys + removed columns and the
  Sample/Snap modes land with the backend (step 4).
- **Operators** ``to_table -> TS<Frame>`` (one bitemporal row per tick;
  ``as_of`` = config override or the evaluation time) and
  ``from_table -> OUT`` (rows applied in order; column resolution **by
  name** — the input-minimum ruling: extra frame columns pass, missing
  required columns throw). The typed ``Frame<Schema>`` wiring marker (exact
  output schemas) follows once a schema-qualified frame consumer exists.
- **A latent registry bug fixed on the way** (armed by the new allocations):
  when ``reset_all_registries``' final ``TypeRegistry::instance().reset()``
  was the process's FIRST registry touch, ``instance()`` seeded before the
  plan-cache clears had any effect ordering-wise, so re-seeded metas could
  reuse freed addresses against stale plan entries ("already registered with
  a different plan", flaky by allocator reuse). ``reset_all_registries`` now
  forces registry construction FIRST. (The stale-pointer-reuse class from
  the plan-registries-clear-on-reset rule.)
- The json/table converter intern tables take **no locks** (the
  ``OperatorRegistry`` precedent: build and evaluation are single-threaded;
  senders never touch them) — the json codec's initial precautionary mutex
  was removed for the same reason.
- **Converters resolve in ``start`` and live in node State** (the lifecycle
  form of the builder pattern — ruled): each json/table operator's ``start``
  hook resolves the composed converter once (``json_converter`` /
  ``table_converter`` interned lookups are start-time only) and parks the
  pointer in ``State<JsonCodecState>`` / ``State<TableCodecState>``;
  ``eval`` is a plain converter invocation with NO lookups, locks or
  branches on the per-tick path. This is the standing pattern for every
  future serializer/codec operator.

Tests: ``tests/cpp/test_table.cpp`` (codec round-trips for atomics and
bundles, bitemporal columns, the input-minimum rule, operator ticks, as-of
override, graph round-trip).

Step 4 — landed
---------------

The Arrow data-frame record/replay backend, model
``record_replay::DATA_FRAME``:

- **The P6 content store** — a graph-scoped, owning type-erased
  ``store::FrameStore`` selected in ``GlobalState``. The handle shares an
  erased context and dispatches through a non-null ``store::FrameStoreOps``
  table; it is not an abstract base class. Native memory, local-filesystem and
  S3 representations remain private and own key validation, serialization,
  compression and immutable-key enforcement. The process fallback uses the
  same erased handle; graph execution first resolves the store belonging to
  that graph.
- **``TraitsView``** — the node-level injectable completing the traits
  primitive: a transparent stateless injectable (the ``SingleShotScheduler``
  pattern) giving hooks ``trait``/``trait_or`` over the owning graph's
  chain; ``fq_recordable_id(TraitsView, id)`` is the node-side resolution.
- **``record`` (frame backend)** — ``requires_`` gates on the model;
  ``start`` resolves the ``TableLayout`` + fq key (explicit
  ``recordable_id`` scalar, defaulting through the trait chain) and creates
  one ``TableRecorder`` over Arrow builders. ``eval`` appends the tick's
  partition, removal and value cells without materialising row ``Value``
  objects, including frame-valued leaves below ``TSD``; ``stop`` finishes one
  complete frame and writes it to the graph store by default.
  Native stores reject an existing key. The in-memory (GlobalState) record
  backends carry the matching ``requires_`` gate on the in-memory models
  (see *In-memory record/replay — sparse vs dense*).
- **``replay`` (frame backend)** — ``start`` reads the frame through the same
  graph store and resolves its columns against the output layout. ``eval``
  applies every row stamped at the current value time and schedules the next
  group absolutely, reproducing recorded timing and gaps. Every native
  recording frame carries its exact projection (stored names and explicit
  omissions) in versioned Arrow schema metadata. Replay checks that descriptor
  at start, so omitting a non-default ``as_of_key`` or ``removed_names`` cannot
  silently discard revision or removal semantics. Unannotated release/0.5 and
  hand-built frames remain readable by explicit names without positional or
  type inference.
- **``replay_const_value(fq_key, meta, tm, as_of)``** — the const read
  (Python's ``replay_const``, a plain function per the const_fn ruling):
  the last row with value-time <= ``tm`` and as-of <= ``as_of``.

The release/0.5 ``DataFrameStorage`` surface is now a compatibility adapter,
not a second recorder. It offers the native bridge only
``store(key, frame)``, ``load(key)`` and ``has(key)``; Python retains ownership
of overwrite policy and receives no native segmentation API. The production
memory, local and S3 paths stay entirely in C++. Store callbacks receive the
Arrow table in the established naive-UTC Python timestamp form, never the
optional user-facing Polars presentation, because Arrow schema metadata is part
of the persisted recording contract. The legacy override registry is translated
at wiring time into explicit native record/replay options.

RFC 0019 completes this backend with per-record column projection,
Tick/Sample/Snap recording, keyed frame expansion, and optional native
segmentation. Tests: ``tests/cpp/test_record_replay_frame.cpp``,
``tests/cpp/test_record_replay_partitioned.cpp``,
``python/tests/test_native_table_recording.py``, and
``python/tests/test_python_frame_store.py``.

Step 5 — landed (first pass)
----------------------------

``stdlib::component<G>(w, "id", inputs...)`` (``lib/std/component.h``) —
Python's ``@component`` as a wiring function. ``G`` is an ordinary graph
struct; the component consults the ambient ``record_replay::scope`` and
wraps per its mode:

- **Record** — every input and the output (``__out__``) is recorded through
  the name-resolved ``record`` (whatever backend the model selects).
- **Replay** — inputs are REPLACED by their recordings (Python parity: the
  live wiring stays but the recorded values win).
- **ReplayOutput** — the output is replaced by its recording.
- **Recover** — each input routes through a component-owned *recovering
  pass-through* that publishes the last recorded value at or before the
  start time from its own ``start`` (the P7 seed — Python's
  ``merge(ts, replay_const(...))`` fused into one node; **no graph-level
  seed state** — only recovering components pay, per Howard's review);
  live ticks override thereafter. ``Recover | Record`` continues
  recording; ``Recover`` combined with replay modes is rejected.
- **Compare** — inputs are replaced by their recordings, the output is
  recomputed, and per-tick equality against the recorded ``__out__`` lands
  in the store — the backtesting regression check (see *The Compare sink*).
- **None** — a plain ``wire<G>``; no wrapping.

Input keys are the graph's ``NamedPort`` names (``arg_<I>`` for plain
``Port`` params — name your component inputs); the fq id chains through
**nested components** via the mode scope (``outer.inner.__out__``),
replacing Python's trait copy-down at the wiring level (runtime graph
traits still serve nodes inside compiled sub-graphs). A recordable id is
required under an active mode (throws otherwise); the consulted
(mode, id) manifests structurally in the wiring, honouring the P3
intern-identity ruling.

Deferred from step 5: scalar compose params on component graphs. Tests:
``tests/cpp/test_component.cpp``.

In-memory record/replay — sparse vs dense (2026-07-18)
------------------------------------------------------

Every in-memory record/replay OPERATOR backend lives in one file,
``lib/std/operators/impl/record_replay_memory_impl.h`` (the sibling of the frame
backend ``record_replay_frame_impl.h``). ``record`` has **two** backends selected
by the record/replay *model* (``record_replay::Config::model``); ``replay`` is a
**single** operator serving both:

- **``record`` under ``IN_MEMORY``** (default) → ``stdlib::sparse_record_impl``:
  **sparse, absolute-time**. Recordings are a ``List`` of
  ``(evaluation_time, delta)`` tuples under ``:memory:<fq_recordable_id>.<key>``;
  they append across runs and tolerate **arbitrary cross-cycle gaps** (real-time
  scheduler alarms, ``@component`` persistence, RECOVER seeding). A bare
  ``record(ts)`` defaults ``key="out"`` and ``recordable_id="nodes.record"`` so
  it round-trips with ``get_recorded_value()`` (which reads
  ``:memory:nodes.record.out`` by default). This is the upstream
  ``_record_replay_in_memory`` semantics.
- **``record`` under ``IN_MEMORY_DENSE``** → ``stdlib::dense_record_impl``:
  **dense, cycle-aligned**. Recordings are a plain-key ``List`` indexed by
  evaluation cycle (``MIN_ST + i*MIN_TD``; a hole = no tick that cycle), read
  back with ``get_recorded_values`` / ``Run.recorded``. This is the graph
  **testing harness** recorder. The raw stateless
  wiring bridge (``_hgraph.Wiring()`` / ``PyWiring``) defaults its GlobalState to
  this model — it *is* the dense harness; a state-seeded wiring inherits its
  GlobalState's model, so real runs (``evaluate_graph`` / ``run_graph``) and
  components stay sparse.
- **``replay`` (one backend)** → ``stdlib::replay_impl``, active under **both**
  in-memory models. Replay is not
  split by the record model — it just replays what was recorded, keyed on the
  presence of a ``recordable_id``: a bare ``replay(key)`` reads the seeded /
  recorded **plain-key** cycle-aligned buffer (``set_replay_values`` /
  ``Run.set_replay`` always seed this layout); an explicit ``recordable_id``
  (component ReplayOutput / Replay / Compare) reads the **sparse absolute-time**
  ``:memory:<fq_recordable_id>.<key>`` recording, replaying each delta at its
  recorded time. The empty-vs-present ``recordable_id`` fully disambiguates, so
  the two paths share one impl with no resolution ambiguity.

The cycle-aligned **buffer-format helpers** (``make_sparse_buffer``,
``dense_entry_delta``, …) both backends and the harness share live in
``lib/testing/record_replay_buffer.h``; the harness **seed/read API**
(``set_replay_values`` / ``get_recorded_values``) is
``lib/testing/record_replay.h``. The layering is
one-directional (``record_replay_buffer.h`` ← ``record_replay_memory_impl.h`` ←
``record_replay.h``): an operator impl and the testing harness depend on a common
base, not on each other.

Prior to this split both in-memory ``record`` backends gated on ``IN_MEMORY`` and
were distinguished only by a ``recordable_id`` argument, and there were two
separate replays — a code smell (a bare ``record(ts)`` in a real-time run wrongly
selected the dense recorder and rejected the large cross-cycle gap). The model
split + single replay make the choice explicit and close the two upstream
wall-clock scheduler tests (``ported/_runtime/test_scheduler.py``). Tests:
``tests/cpp/test_record_replay.cpp``, ``test_erased_wiring.cpp``,
``python/tests/test_bridge.py``.

The Compare sink (landed)
-------------------------

The Q-compare ruling realised: ``compare(lhs, rhs, recordable_id="")`` is a
sink recording per-tick equality (erased ``ValueView::equals``) into a
bitemporal ``Bool`` frame via the ``FrameRecorder``, written through the
**registered frame store** (P6) at stop under ``fq.__compare__`` — the
store is the pluggable seam, so one implementation serves every model.
Activation with a one-sided value IS a mismatch (one series produced where
the other did not) and is recorded as a failure — never skipped.
``record_replay::comparison_summary(fq_key)`` reads results back as
``(compared, mismatches)``.

P1 — const-evaluable operators (landed)
---------------------------------------

The const_fn ruling realised without a node-class port: an operator impl
may declare ``static Value const_eval(const TSValueTypeMetaData
*resolved_output, OperatorCallContext)`` — detected at registration like
``requires_`` and stored as the overload's **const kernel**.
``OperatorRegistry::evaluate_const(name, args, expected_output)`` resolves
the overload exactly as ``wire`` does (normalisation, defaults,
``requires_`` gates, expected-output unification) and invokes the kernel
directly — Python's dual-mode ``@const_fn`` eager call. The wired form is
the same impl's ordinary ``eval``. Registered const-evaluables:
``replay_const(key, recordable_id, tm=MAX_DT)`` (DATA_FRAME-gated; the
eager call REQUIRES an explicit ``recordable_id`` — no graph traits exist
outside a graph; the wired form resolves through traits and cuts at the
start time) and ``from_table_const(value: Frame)`` (the frame's last row
at the resolved output schema). Tests: ``tests/cpp/test_const_eval.cpp``.

Rulings (Howard, 2026-07-04)
----------------------------

1. **Q-const_fn — ruled: not ported.** ``const_fn`` does not exist in C++;
   const-evaluable operator registration (P1) is the design.
2. **Q-model — ruled: P2 approved.** The record/replay model is explicit
   wiring-time configuration; Python's imperative setters become bridge
   shims. Changing the model mid-wiring is deliberately unsupported.
3. **Q-modes — ruled: P3 approved.** The mode scope rides the wiring-scope
   machinery and is folded into intern identity.
4. **Q-table-surface — ruled: P4 approved.** User-visible ``to_table``
   stays; backends drive the serializer ops / Arrow builders directly.
5. **Q-arrow-dep — ruled:** Arrow is a **formal dependency** and the table
   is a **first-class type abstracted behind ``Frame``**, with the flexible
   schema semantics recorded in P4: schema-less ``Frame`` is legal; an input
   schema is a *minimum* (at-least-these-columns); an output schema is
   *exact*.
6. **Q-compare — ruled:** results flow through a **registered, type-erased
   content store** (P6), shared with related record/replay code.
7. **Q-recover — ruled: most efficient approach.** Python's merge-based
   seeding was expedience ("kept simple because I wanted something I was
   reasonably sure would work"); C++ recovery seeds outputs/recordable state
   directly at start (P7).

Step 6 — the TABLE parity surface (design, 2026-07-11)
------------------------------------------------------

The user-visible ``to_table`` promised by P4 ("a ``TS`` of row values")
lands as the **tuple-row protocol**, replacing the step-3 first-pass
``to_table -> TS<Frame>`` shape (that emission path stays as the
record/replay backend's fused Arrow route; the frame round-trip remains
reachable through ``TS[Frame[...]]`` payloads below). Python-parity rules:

- **Row layout** (synthesised once per resolved input TS schema + configured
  bitemporal names, the serializer-ops pattern):
  ``[date_key, as_of_key, {removed, *key-cols}(per TSD level), *value-cols]``.
  Value columns are the ``TableConverter`` flattening (bundles dotted).
  Partition-key columns follow Python's naming: level *N* of a TSD
  contributes ``__key_N_removed__`` plus ``__key_N__`` (atomic key),
  ``__key_N_<i>__`` (tuple key, per index) or ``__key_N_<field>__``
  (compound key, dotted for nesting).
- **Row value** = a fixed **Tuple** value over the layout's leaf schemas.
  A time-series type with no partition keys and no multi-row payload emits
  ``TS<tuple[...]>`` — one row per tick. Partitioned (any TSD level) or
  multi-row (``Frame``-valued ``TS``) types emit ``TS<tuple[tuple[...], ...]>``
  (the variadic-tuple/List value). The output schema is computed at wiring
  by the ``to_table`` compose (the window-operator precedent).
- **Nullable cells are tuple field validity.** Upstream writes ``None`` into
  unmodified/absent cells; here an unset tuple field IS that ``None`` (the
  established field-validity concept; ``to_python`` already reads holes back
  as ``None``). The value layer's sequence-fill (``Tuple``-from-python)
  learns the reverse rule: a ``None`` element marks the field UNSET rather
  than erroring. No new value kind, no ``Any`` boxes on the hot path.
- **Modes** (``ToTableMode`` — a first-class C++ enum ``Tick=1, Sample=2,
  Snap=3`` mirroring Python's ``auto()`` values; the operator takes it as a
  ``TS`` input defaulting to ``Tick``): *Tick* writes modified leaves only
  (unmodified cells stay unset); *Sample* writes the full current value for
  every modified/removed partition entry; *Snap* writes full values for all
  current entries and no removals. Removal rows set the level's removed flag
  ``True`` and leave deeper key/value cells unset.
- **``from_table``** reverses: each row applies as this tick's delta at the
  resolved output (last-write-wins per tick for whole-value ``TS``; removed
  flags map to key removal on TSD outputs).
- **``table_schema``** is const-evaluable in spirit: the C++ layout is the
  single source (bridge introspection over the synthesised layout); the
  Python ``TableSchema``/``make_table_schema`` classes are thin declarative
  mappings from layout leaf kinds to Python types (C++-first API ruling —
  no schema derivation logic in Python).
- **``Frame``-valued ``TS``** payloads are multi-row: ``to_table`` explodes
  the tick's frame into one row per frame row (shared bitemporal cells);
  ``from_table`` rebuilds the frame. The typed wiring marker
  ``Frame[Schema]`` maps to an interned typed frame meta
  (``TypeRegistry::frame(bundle)`` — the ``series(element)`` pattern:
  shared storage plan + ops, distinct meta carrying the column schema).
- **Data-frame convenience operators** (Python's
  ``to_data_frame``/``from_data_frame``/``group_by``): the same layout
  machinery with a plain ``date`` column and no ``as_of``;
  ``to_data_frame`` emits one-tick frames, ``from_data_frame`` replays a
  frame VALUE by its date column (TSD forms take ``key_col``). These ride
  the tuple-row/frame codecs — no third serialisation path.

The TS-level walkers live with the operator impls (the ``json_ts_detail``
precedent): layout synthesis + row emission in ``lib/std/operators/impl``
(interned per (ts-schema, date-key, as-of-key); cleared on registry reset
per the plan-registries rule); the value-level ``TableConverter`` is
unchanged underneath.

Step 7 — native segmented recordings
------------------------------------

``record`` normally retains one run in Arrow builders and writes one immutable
frame at ``stop``. Native memory, local-filesystem, and S3 stores additionally
honour the wiring-time ``flush_rows`` and ``flush_interval`` thresholds. A
flush happens only after the current evaluation tick has been emitted, so the
rows of one frame-valued tick are never split between segments.

A segmented recording uses an immutable base-key marker, independently valid
frames at ``<key>.0``, ``<key>.1``, and so on, and a ``<key>.complete``
manifest after a successful stop. Replay needs neither enumeration nor the
manifest: it loads one segment, releases it after its last time group, and
then probes the next number. An interrupted run therefore replays every fully
published segment and nothing torn. A writer refuses either a claimed base key
or a pre-existing ``.0`` legacy/segment key.

Segmentation is intentionally not part of ``FrameStoreOps``. Core identifies
its own immutable native stores without changing that public ops-table ABI.
Python and other custom frame stores remain the compatibility seam promised by
RFC 0019: one complete ``store``/``load``/``has`` frame operation per run,
even when the call supplied flush thresholds.

Step 8 — public table type operations
-------------------------------------

``types/table_type_ops.h`` contains the installed extension contract:
``TableLayout``, ``TableRowSink``, ``TableRowSource``, recording projection
types, and the passive ``TableTypeOps`` table. An extension may register a
static, complete ``describe``/``emit``/``apply`` table for an exact resolved
time-series schema before wiring. The registry selects it while building the
interned layout; evaluation performs no lookup and holds no strategy-specific
container.

Exact child strategies compose beneath built-in ``TSB`` and ``TSD`` layouts.
The builder interns the child's own plan, projects its columns into the parent,
and records that projection in the parent plan. A dedicated boolean column
records whether the child emitted a row; presence is not inferred from payload
nullability, so strategies with no value columns and valid all-null rows retain
their update semantics. Emission and application then delegate through the
child's selected operations without another registry lookup. An embedded
strategy must describe and emit a single row; combining a multi-row child with
sibling structural fields has no unambiguous row product and is rejected while
the layout is built.

Direct tuple-row application and persisted Arrow replay both adapt their
input to ``TableRowSource`` and call the selected ``apply`` function. This is
the important inverse guarantee: an extension does not acquire a separate
stored-replay implementation. Default and moved-from-style states use a
canonical table whose three function pointers are non-null and fail with a
diagnostic when invoked. Registry reset clears schema-addressed overrides and
cached layouts because their metadata addresses are no longer valid.

The installed-SDK consumer includes this header and checks the passive,
non-polymorphic contract. Native tests register a custom scalar time-series
strategy and exercise both direct ``to_table``/``from_table`` and
record/store/replay paths.
