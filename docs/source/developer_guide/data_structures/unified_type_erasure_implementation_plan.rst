Unified Type Erasure Implementation Plan
========================================

Status And Scope
----------------

This chapter is the proposed delivery plan for :doc:`unified_type_erasure`.
Each milestone must be independently buildable, testable, reviewable, and
reversible.  The migration must not require a flag day across values,
time-series, nodes, graphs, and the Python bridge.

The plan deliberately starts with a normalised representation:

* ``SchemaHeader`` owns semantic family, semantic kind, schema ABI, and label;
* ``TypeRecord`` owns role, record and ops ABI, capabilities,
  implementation label, plan, ops, and debug metadata;
* ``TypeRecord`` refers to ``SchemaHeader`` rather than copying schema fields;
* ``AnyPtr`` contains only a type-record pointer and a data pointer;
* a combined family/role/kind tag is computed, not stored.

This removes the overlap identified during design review.  A packed tag may be
cached later only when a benchmark demonstrates that the schema dereference is
material.  It is not part of the initial ABI.

Delivery Method
---------------

Two model roles should be used throughout the migration.  They describe
capability and reasoning budget rather than a particular vendor or release.

Implementation model
   A cost-effective coding model running at normal or medium reasoning.  It is
   responsible for a bounded change, focused tests, mechanical migrations,
   build fixes, and a factual implementation report.  It receives an approved
   contract and must stop rather than alter cross-family semantics.

Review model
   The highest-reasoning coding model available.  It defines or approves the
   milestone contract, reviews the actual repository diff, checks ownership and
   ABI invariants, examines generated or benchmarked behaviour where relevant,
   and accepts or rejects the milestone.  It does not rely on the implementation
   model's summary.

If the execution environment exposes reasoning levels rather than separate
models, use medium for implementation and high or maximum for review.  The
important property is independent re-evaluation with a larger reasoning budget,
not the model name.

The workflow for every milestone is:

1. The review model writes a short implementation packet: permitted scope,
   public contract, invariants, required tests, benchmarks, and stop conditions.
2. The implementation model changes only that scope and leaves the result
   uncommitted.  It runs the requested checks and reports failures and measured
   results without interpreting them as acceptance.
3. The review model reads the code and complete diff, runs or inspects the
   checks, and reviews data layout, lifetime, and migration compatibility.
4. Rejected work returns to the implementation model with concrete findings.
   A material design change returns to step 1.
5. A human-approved milestone is committed as one coherent change before the
   next migration begins.

The implementation packet should include exact file or subsystem boundaries.
It should also state that unrelated worktree changes must not be modified or
included.  This makes a lower-cost implementation model effective without
giving it an ambiguous architecture task.

Cross-Cutting Gates
-------------------

The following gates apply to every milestone:

Pure C++ build
   Normal CMake configure, build, test, and install remain independent of
   Python, nanobind, and Python package installation.

Optional Python build
   When a migrated family crosses the bridge, the supported Python build and
   compatibility tests also pass with the bridge options enabled.

Layout
   New common structures have explicit standard-layout and size assertions.
   ``AnyPtr`` and every typed pointer remain exactly two machine words.

Lifetime
   No borrowed pointer constructs, destroys, subscribes, unsubscribes, stops,
   or erases its target.  Existing graph, owner, and slot protocols remain the
   lifetime authorities during migration.

Identity
   Schemas and type records are immutable and interned.  There is only one
   canonical type-record identity for a resolved representation; compatibility
   code must not establish a second sidecar identity.

Diagnostics
   Invalid family, role, schema, plan, or ops combinations fail at factory or
   conversion boundaries.  Debug builds provide assertions in addition to
   normal error handling where corrupt state would otherwise be undefined.

Performance
   The relevant baseline benchmark and allocation count are captured before a
   hot path is migrated.  Review compares distributions or repeated samples,
   not a single timing.  Regressions are investigated rather than hidden by a
   broad tolerance.

Documentation
   Names follow Schema, Plan, Ops, Type Record, Pointer, Owner, View, Builder,
   and Registry/Factory.  Temporary compatibility names are marked as such.

Milestone 0: Inventory And Baseline
-----------------------------------

Purpose
   Establish evidence before changing layouts or ownership.

Implementation model work
   Produce an inventory of every ``TypeBinding`` specialisation,
   ``StorageRef`` and ``StorageHandle`` use, family registry, builder, view, and
   pretty-printer entry point.  Add missing size assertions and focused
   benchmarks for value access, scalar TS access, composite TS access, node
   evaluation, graph construction, and a nested-graph lifecycle.  Record
   allocation counts where tooling permits.

Review model work
   Classify each current object as Schema, Plan, Ops, Type Record, Pointer,
   Owner, View, or Builder.  Resolve disputed vocabulary and approve the
   initial family, role, and kind tables.  Identify code which currently relies
   on a binding's concrete C++ type or on ``StorageHandle`` borrowed state.

Acceptance
   The inventory accounts for all binding aliases and runtime families; tests
   assert current pointer and handle sizes; benchmarks run repeatably in the
   canonical local Linux VM and the selected macOS configuration.  Windows is
   a best-effort compile and test target, not a Milestone 0 performance or
   progression gate.  No runtime behaviour is changed.

Recommended model allocation
   A cost-effective model performs searches, tables, assertions, and benchmark
   harness work.  The highest-reasoning model owns classification and baseline
   adequacy.

Milestone 1: Common Records And Registry
----------------------------------------

Purpose
   Introduce self-describing cold metadata without migrating a runtime family.

Implementation model work
   Add ``TypeFamily``, ``TypeRole``, common ABI constants, ``SchemaHeader``,
   ``TypeCapabilities``, and ``TypeRecord``.  Implement type-record validation
   and interning using the repository's existing registry ownership pattern.
   Use mock schemas, plans, and ops in unit tests; do not route production views
   through the new records yet.

Technical model
   Prefer composition or a pointer to ``SchemaHeader`` over inheritance and
   RTTI.  A family schema can contain the header as its first standard-layout
   member or own a stable referenced header.  The choice should follow current
   schema allocation and interning after the Milestone 0 inventory.  The
   ``TypeRecord`` itself is one non-template structure.

Compatibility rule
   Do not build a long-lived map from an old binding identity to a second type
   record identity.  During later family migrations, typed compatibility is a
   non-owning accessor over the canonical ``TypeRecord`` or a temporary
   standard-layout wrapper containing that record, never a separately interned
   descriptor.

Acceptance
   Tests cover interning identity, concurrency, invalid schema/role/ops
   combinations, string lifetime, ABI rejection, and registry reset policy.
   The normal runtime is unchanged and baseline performance is unchanged.

Recommended model allocation
   The review model specifies field ownership, layout, registry lifetime, and
   validation rules.  A cost-effective model implements the approved records
   and tests.  The review model checks standard-layout guarantees and every
   unchecked cast before acceptance.

Milestone 2: Generic And Typed Pointers
---------------------------------------

Purpose
   Prove the compact pointer and conversion model independently of existing
   views and owners.

Implementation model work
   Implement ``AnyPtr`` and ``TypedPtr<Family, Role>`` against mock and common
   records.  Add read-only, writable, and mutation access modes only where they
   correspond to existing semantics.  Implement explicit checked narrowing and
   free widening.  Do not add ownership or destruction to either pointer.

Technical model
   Start without pointer tagging if doing so makes correctness and debugger
   bring-up clearer.  Add tag bits in a separate patch after alignment is
   asserted on every platform.  Both representations must remain two words;
   temporary access-mode storage which increases the pointer size is not an
   acceptable endpoint.

Acceptance
   Tests cover unbound, typed-null, live, and invalid states; const and access
   conversions; family and role mismatch; pointer equality; size and alignment;
   and conversion with corrupted magic or ABI.  Sanitizer builds find no
   lifetime or alignment issue.

Recommended model allocation
   The review model approves null and conversion semantics.  A cost-effective
   model implements the value types and exhaustive table-driven tests.  Use the
   review model again before enabling pointer tagging.

Milestone 3: Value-Family Pilot
-------------------------------

Purpose
   Migrate the simplest mature family and verify that the common model works on
   a real hot path.

Implementation model work
   Give value schemas a ``SchemaHeader`` and make the value factory intern
   canonical ``TypeRecord`` objects.  Replace value binding access with typed
   accessors over ``TypeRecord``.  Migrate ``ValueView`` to the two-word typed
   pointer while preserving its mutability rules and public behaviour.  Cover
   atomic, tuple, list, mapping, and Python-backed value representations used by
   existing tests.

Technical model
   ``schema->label`` is the only semantic label.  Compact versus non-compact,
   native versus Python-backed, or other representation detail belongs in the
   implementation label, plan, ops, or capabilities.  Do not cache family or
   kind on the type record during this pilot.

Acceptance
   Existing value and operator tests pass; value-view size does not grow;
   schema and type-record interning identities are stable; mutation protection
   is unchanged; and value access benchmarks and allocation counts remain
   within the pre-approved baseline envelope.

Recommended model allocation
   A cost-effective model can perform the implementation after the factory and
   typed-access contract is approved.  The review model examines the resulting
   generated hot path or representative assembly as well as the source diff.

Review Gate A
~~~~~~~~~~~~~

Pause after the value pilot.  The high-reasoning review determines whether the
normalised schema/type-record split is ergonomic in real code.  Field movement,
naming, or typed-access changes are still inexpensive here.  Do not begin the
time-series migration until this gate is accepted.

Milestone 4: Time-Series Roles
------------------------------

Purpose
   Validate the distinction between one semantic schema and multiple runtime
   representations.

Iteration 4A
   Migrate scalar TS data, input, and output.  One TS schema must resolve to
   separate ``Data``, ``Input``, and ``Output`` type records with the correct
   plans and ops tables.

Iteration 4B
   Migrate TSB and fixed TSL composites, including child type records, named
   TSB identity, root-plan offsets, and Data/Input/Output topology.

Iteration 4C
   Migrate keyed and reference types, including TSD, TSS, and REF restrictions.
   Preserve subscription, unsubscription, delete, and erase semantics.
   Implemented with role-specific root, embedded, key-set, value, proxy, and
   reference-alternative records. Slot removal stops and retains storage;
   erase performs destruction after observers have processed the removal.

Iteration 4D
   Migrate dynamic TSL and TSW. Preserve dynamic-list growth and element
   lifetime semantics, and keep window storage and eviction behaviour
   unchanged.

   Implemented: dynamic ``TSL`` and tick/duration ``TSW`` expose canonical
   Data, Input, and Output records (originally ABI 3; advanced to ABI 4 by
   the pointer/owner separation). Role-specific root and embedded
   labels share one physical plan per schema. Dynamic-list storage binds its
   one-word element type on first growth, remains grow-only, and invalidates
   owned descendants before their handles are destroyed. Owned and peered
   dynamic roots are supported; non-peered dynamic structural prefixes remain
   rejected. Window removal queries at the data layer require an explicit
   evaluation time, while input endpoints supply the current cycle. Window
   buffer and eviction algorithms are unchanged. This records implementation
   status only; review acceptance and commit status remain open.

Technical model
   TS, TSS, TSL, TSB, TSD, and REF remain schema kinds.  Data, Input, and Output
   remain type-record roles.  Role-specific ops accessors validate both family
   and role.  A role is not encoded by creating a duplicate semantic schema.

Acceptance
   All existing C++ and Python bridge time-series tests pass.  Each schema can
   enumerate or resolve its supported roles.  Composite child navigation is
   correct.  Slot-backed keyed types stop on delete and destruct on erase.
   Access benchmarks and nested keyed allocation counts show no unexplained
   regression.

Recommended model allocation
   The review model authors the contract for each iteration.  A cost-effective
   model implements 4A and, after review, repeats the established pattern for
   4B and 4C.  Each iteration receives a separate high-reasoning review; do not
   ask one implementation agent to migrate the entire family in a single diff.

Review Gate B
~~~~~~~~~~~~~

Review whether family, role, and kind are sufficient across every migrated TS
shape.  This is the earliest point at which a cached classification may be
benchmarked.  It should be rejected unless measurements show a meaningful
regression caused specifically by following ``TypeRecord::schema``.

Milestone 5: Node And Graph Families
------------------------------------

Purpose
   Make compile-time-known runtime families rich in C++ while remaining
   generically inspectable.

Iteration 5A
   Migrate node schemas, factories, builders, and runtime pointers.  Native
   compute, sink, generator, service, and system nodes use ``NodePtr`` on typed
   paths and ``AnyPtr`` only at genuinely generic boundaries.

   Implemented: ``NodeTypeMetaData`` carries the common node schema header;
   ``NodeTypeRef`` is the one-word canonical Runtime-role identity; and
   ``NodeView`` carries ``NodePtr``.  ``NodeBuilder``, graph node-location
   tables, nested graph parents, and node-owned endpoint parent links use the
   typed record/pointer contract directly.  The former ``NodeTypeBinding`` and
   ``NodeStorageRef`` representations have been removed.  Existing
   ``NodeKind`` values describe the current compute, source/generator, sink,
   nested/system, and service implementations without adding speculative
   sub-families.  Review accepted and committed.

Iteration 5B
   Migrate Python-authored node implementations to the same Node family and ops
   contract.  Python remains an implementation selected by the type record,
   not a separate runtime type system.

   Implemented: node descriptors can supply the TypeRecord implementation
   label, static C++ node implementations can declare it without changing the
   common runtime, and builder-derived node variants preserve it.  The Python
   compute, sink, and generator trampolines identify themselves as
   ``hgraph.python.compute``, ``hgraph.python.sink``, and
   ``hgraph.python.generator`` while retaining the common Node family, Runtime
   role, ``NodeTypeRef``, ``NodePtr``, storage-plan, and ``NodeOps`` paths.
   Wiring-time Python ``Port`` diagnostics expose the producing node's common
   record fields for inspection.  Review accepted and committed.

Iteration 5C
   Migrate graph schemas, builders, instances, executors, and clocks in that
   order.  Keep distinct families only where the ops ABI and lifecycle are
   genuinely distinct.

   Implemented: Graph, Executor, and Clock remain distinct common families
   because their ops and lifecycle contracts differ. ``GraphTypeRef``,
   ``ExecutorTypeRef``, and ``ClockTypeRef`` are one-word canonical identities;
   their views carry the corresponding two-word typed pointers and their owners
   store ``TypeRecord`` directly. Root and nested graph records are compiled
   together over one semantic graph schema and differ only by plan, ops, and
   implementation label. Graph and executor builders cache their compiled
   records and invalidate them when type-shaping configuration changes.
   Simulation, realtime, and mock clocks are read-only projections over
   executor storage sharing one clock schema. The former graph, executor, and
   clock ``TypeBinding`` / ``StorageRef`` identities have been removed. Review
   accepted and committed.

Acceptance
   Mixed native/Python graph construction and execution tests pass for compute,
   sink, generator, and service nodes.  Generic diagnostics can accept any
   migrated pointer.  Typed compile and run paths do not repeatedly check the
   family.  Pure C++ builds contain no Python dependency.

Recommended model allocation
   A cost-effective model handles one iteration at a time after a review-model
   contract.  The highest-reasoning model reviews the Python boundary, graph
   ownership, and the decision to retain or combine Executor and Clock families.

Review Gate C
~~~~~~~~~~~~~

Review all family boundaries using real call sites.  Remove speculative family
or role values which have no distinct schema or ops ABI.  Confirm that a typed
``NodePtr`` or ``GraphPtr`` provides the navigation Alex needs without storing
additional words.

Completed review
   Value, TimeSeries, Node, Graph, Executor, and Clock each have a distinct
   schema layout and ops ABI. Executor and Clock remain separate because the
   former owns mutable run lifecycle while the latter is a read-only projection
   over that storage. Root/nested graphs and executor modes remain records or
   kinds within their existing families. Roles are limited to the implemented
   Instance, Data/Input/Output, and Runtime storage contracts. Generic family
   checks occur at ``AnyPtr`` narrowing and registry validation; typed runtime
   navigation follows the record directly. ``NodePtr`` and ``GraphPtr`` remain
   two words with no cached schema or ops pointer.

Milestone 6: Debug Metadata And Pretty Printers
-----------------------------------------------

Purpose
   Deliver the original debugging objective without requiring debugger calls
   into a stopped process.

Iteration 6A
   Add shallow GDB and LLDB support for ``SchemaHeader``, ``TypeRecord``,
   ``AnyPtr``, and typed pointers.  Display validity, access, semantic label,
   implementation label, family, role, kind, ABIs, plan, ops, and data address.

   Implemented: GDB and LLDB adapters read the common ABI without inferior
   calls and share debugger-independent validation and formatting. Summaries
   distinguish invalid, unbound, typed-null, live, and malformed state; pointer
   expansion navigates to the record and schema. The prior binding-specific
   payload guesses have been removed pending descriptor-backed deep traversal.
   Snapshot tests cover corruption and overlapping family-specific kinds, and
   the adapter is exercised against live C++ objects on macOS. Review accepted
   and committed.

Iteration 6B
   Define the stable data-only ``DebugDescriptor`` and implement atomic and
   fixed-composite navigation.

   Implemented: the versioned 64-byte descriptor distinguishes opaque, atomic,
   fixed-composite, and reserved dynamic layouts. Atomic representation tags
   come from C++ registration traits rather than labels. Fixed fields carry a
   child ``TypeRecord``, physical plan offset, and validity bit; tuple/bundle
   descriptors publish the validity-word location. The value plan factory owns
   pointer-stable descriptors and attaches them to canonical records. GDB and
   LLDB decode supported atomics and synthesize child ``AnyPtr`` values without
   inferior calls. A debugger fixture covers recursive children, unset fields,
   and static teardown. Review accepted and committed.

Iteration 6C
   Add sequences, keyed slots, nodes, graphs, and nested-graph navigation.
   Unknown or unsupported representations remain labeled opaque values rather
   than being guessed from memory.

   Implemented: the versioned dynamic layout describes contiguous and stable
   pointer-slot storage, including fixed/dynamic size, stride, ring head, key
   storage, slot-state bitmap, and embedded erased owners. Slot pointer/state
   storage now has an explicit data-only ABI while reducing per-store
   bookkeeping size. Value descriptors cover fixed and dense dynamic
   sequences plus mutable keyed containers. Graph descriptors publish direct
   node allocations; node descriptors publish state/scalar owners and nested
   graph owners. Map and mesh expose their constructed entry slots, retained
   stopped graphs, and omit erased slots through the slot lifecycle bitmap.
   GDB and LLDB traverse each supported form without inferior calls. Nullable
   dynamic validity and specialized endpoint owners remain explicitly opaque.
   Review accepted and committed.

Technical model
   Printers dispatch from common numeric fields and descriptor data, not C++
   template spellings or private container member names.  Tests decode captured
   structures without calling inferior functions and cover both live-process
   and core-file-compatible paths.

Acceptance
   The same erased pointer can be inspected generically in GDB and LLDB.  Deep
   traversal is correct for each supported layout and fails safely for corrupt,
   version-mismatched, and unknown data.  Release builds retain mandatory
   shallow metadata.

Recommended model allocation
   The review model owns the descriptor ABI and decides which offsets are
   stable contracts.  A cost-effective model is well suited to printer and
   fixture implementation once that format is fixed.  The final review should
   be performed by someone using the printers interactively, not only by tests.

Milestone 7: Separate Pointer And Owner
---------------------------------------

Purpose
   Remove the ambiguous borrowed mode from ``StorageHandle`` after compact
   pointers are established throughout the runtime.

Implementation model work
   Introduce an owner containing only inline or allocated ownership states and
   migrate borrowed call sites to ``AnyPtr`` or a typed pointer.  Preserve
   allocator policy and small-storage optimisation.  Make owner-to-pointer
   conversion explicit and non-owning.

Technical model
   This milestone changes lifetime expression and must not be mixed with a
   storage-plan redesign.  An owner destroys exactly once.  A pointer never
   destroys.  In-place graph storage is governed by its graph plan or slot
   store and exposes a pointer, not a synthetic owner.

Acceptance
   Focused tests cover inline, heap, moved, released, and externally placed
   storage; exceptions during construction; owner destruction; dangling-use
   detection where available; and allocator propagation.  Sanitizers and the
   full nested-graph suite pass.  Owner size and allocation behaviour are
   measured against Milestone 0.

Implemented
   ``MemoryUtils::ErasedOwner`` replaces ``StorageHandle`` and has only empty,
   owning-inline, and owning-heap states; tag value three is reserved. The
   owner retains the existing allocator, SBO, deep-copy, move-transfer,
   typed-null, and construction rollback behavior in the same three-word
   layout. Its reference factories and borrowed-state queries are removed.

   ``Value`` and all node, graph, executor, mock, and dynamic-TSL owners use
   ``ErasedOwner``. Destructive TS assignment dispatches through an rvalue
   writable ``ValueView``; ``Value&&`` remains a convenience overload and
   read-only sources fail before dispatch. This removes the former
   ``Value::reference`` synthetic owner and advances ``TSDataOps`` to ABI 4.

   ``GraphValue`` begins with a common two-word ``GraphPtr`` and carries an
   owner only for normally allocated graphs. Externally placed nested graphs
   carry no owner and are destroyed by the graph/slot protocol. Graph movement
   rebinds the pointer to moved owner storage, while external pointers transfer
   without changing their target. Debug descriptors use embedded-pointer
   fields for nested/switch graphs and pointer elements for map/mesh slots.
   ``GraphValue`` grows from four to five words; ``ErasedOwner`` stays three
   words and all typed pointers stay two. Review accepted and committed.

   A same-host Release comparison against pre-milestone commit ``3092e668``
   produced identical allocation counts in every type-erasure benchmark. Small
   graph construction retained 22 allocations per operation and alternating
   switch lifecycle retained 1,445.05; bytes increased by exactly 8 and 24 per
   operation respectively, matching the additional ``GraphValue`` pointer word
   in the benchmarked layouts. Hot reads and steady window operations remained
   allocation-free, and timing sample ranges showed no attributable regression.

Recommended model allocation
   The highest-reasoning model must write the lifetime state machine and review
   every ownership transition.  A cost-effective model can perform call-site
   migration and tests after the core owner is approved.  If the core state
   machine itself requires invention during implementation, use the
   high-reasoning model for that patch rather than delegating it.

Milestone 8: Nested Graph And Slot Validation
---------------------------------------------

Purpose
   Prove that common pointers describe in-place nested instances without
   weakening the established storage protocol.

Implementation model work
   Audit switch, map, mesh, and other nested graphs.  Ensure graph plans expose
   typed pointers into pre-allocated memory, keyed instances use slot observers,
   delete performs stop and unsubscription, and erase performs destruction and
   slot reuse.  Add debug descriptors for active, stopped/deleted, and erased
   states where applicable.

Acceptance
   Capacity growth, key churn, engine-cycle retention, switch alternation, stop,
   erase, and exception tests pass under sanitizers.  Allocation tests show that
   static nested state is placed in graph or slot memory and that steady-state
   execution does not introduce descriptor or pointer allocation.

Implemented
   The audit covers single nested graphs, switch, map, mesh, and dynamic reduce.
   Fixed nested graphs remain in the parent node storage plan. Switch uses two
   maximum-layout graph regions. Map and mesh co-locate entry headers and child
   graph payloads in stable slots, and reduce alternates two positional banks.
   ``GraphValue`` exposes pointers into those regions without owning them.

   Map and mesh removal tests now observe both active and constructed graph
   counts. They prove logical removal stops a child while its entry remains
   constructed, and physical slot erase performs destruction later. Switch
   tests prove that three alternating activations use addresses A, B, A. Slot
   tests cover capacity growth without relocation, bank swapping without
   relocation, destructor-on-erase behavior, and rollback after a throwing
   entry constructor. Direct map key-source replacement now alternates two
   stable stores so replacement slot ids cannot force same-cycle destruction.

   Allocation cleanup removes temporary boundary-path vectors, replaces
   forwarding-chain allocation with constant-storage cycle detection, caches
   mesh rank ordering after capacity growth, represents the current mesh key as
   a borrowed ``ValuePtr``, and removes stale reduce leaves in place. A reusable
   Release benchmark schedules already-built map, mesh, and reduce nodes for
   20,000 steady-state scans with zero allocations. On the review Mac its
   seven-sample median was 1,349.646 ns per scan. Existing benchmark allocation
   counts remained stable. The Release and combined ASan/UBSan suites each pass
   975 test cases and 9,476 assertions. Review accepted; this milestone is
   committed separately from the removal and ABI review.

Recommended model allocation
   A cost-effective model can extend established tests and fix local accessor
   use.  The highest-reasoning model reviews slot lifetime, placement reuse, and
   every new owning allocation.  Any change to delete/erase semantics requires
   a new review contract before implementation continues.

Milestone 9: Removal, Performance, And ABI Review
-------------------------------------------------

Purpose
   Remove the transitional system and decide which representation is ready to
   become stable.

Implementation model work
   Remove obsolete type-binding aliases, side paths, old printer dispatch, and
   deprecated terminology.  Update all design and developer documentation.
   Run the full platform matrix, sanitizers, benchmarks, allocation tests, and
   Python compatibility suite.

Review model work
   Search independently for old binding identities and unsafe casts.  Compare
   final layouts and benchmark distributions with Milestone 0.  Decide whether
   ABI fields are ready to freeze, whether deep debug metadata is optional, and
   whether a cached classification tag has earned its cost.

Acceptance
   There is one canonical schema identity and one canonical type-record
   identity per representation.  All borrowed erased pointers use the common
   two-word model.  No compatibility registry or duplicate semantic label
   remains.  C++ and optional Python checks pass on the required Linux and
   macOS platforms; Windows results are reported on a best-effort basis and do
   not block progression.  Measured regressions have an explicit accepted
   explanation.

Implemented
   The final time-series migration removes ``TypeBinding``,
   ``TSDataBinding``, the tagged ``TSStorageTypeRef``, their compatibility
   registry, and the generic binding-based ``MemoryUtils::StorageRef``.  A
   ``TSRoleTypeRef`` now carries the only time-series representation identity:
   the canonical interned ``TypeRecord`` for a schema, endpoint role, storage
   plan, and ops table.  Factories, nested child layouts, deltas, proxies,
   inputs, outputs, and parent links all pass that record directly.  Searches
   of production and C++ test sources find no remaining legacy identity or
   accessor path.

   Every borrowed time-series data cursor is now the common two-word
   type-record/data-pointer representation.  ``TSDataStorageRef`` and
   ``TSDataView`` shrink from three words to two; ``TSOutputHandle`` from four
   to three; ``TSOutputView`` from five to four; ``TSInputView`` from ten to
   eight; and ``TimeSeriesReference`` from six to five.  Owning ``TSData``,
   ``TSOutput``, and ``TSInput`` layouts remain three, five, and seven words.
   Compile-time assertions cover generic and specialised cursors as well as
   all endpoint view and owner layouts.

   On macOS, AppleClang 21 passes the warnings-as-errors Debug build and the
   combined ASan/UBSan suite (973 cases, 9,423 assertions).  The CPython 3.12
   stable-ABI bridge passes 947 compatibility tests with 38 skips and four
   expected failures.  A Python-disabled Release build installs successfully,
   and Sphinx passes with warnings treated as errors.

   The required Linux validation uses the Ubuntu 24.04 OrbStack VM and GCC
   13.3.  The warnings-as-errors Release build passes 973 cases and 9,417
   assertions.  It also builds a ``cp312-abi3-linux_x86_64`` wheel and passes
   the same 947-test Python compatibility suite.  GCC's dangling-reference
   analysis found and removed four references taken through temporary type
   handles.

   Best-effort Windows validation uses the MinGW-w64 x86-64 Windows target
   with GCC 16.1.  Runtime, wiring, stdlib, and all five C++ test object groups
   compile as C++23 with warnings as errors, including the ABI assertions.  It
   found a public header that depended on the precompiled header and made a
   slot-bitmap preservation copy explicitly capacity-bounded.  MSVC linking
   and execution still require GitHub Actions after the milestone commit.

   Immediate pre-milestone Release comparisons retain the same allocation
   counts.  Across repeated review-Mac samples, small-graph construction was
   0.3% to 3.0% faster, nested steady scheduling 5.9% to 7.6% faster, and
   switch lifecycle ranged from 2.0% faster to 1.0% slower.  Scalar,
   fixed-composite, and dynamic-list reads were 4.9% to 5.4%, 2.8% to 4.0%,
   and 5.4% to 9.1% slower.  On Linux, scalar and fixed-composite reads
   improved by 12.5% to 17.0% and 1.5% to 3.5%; dynamic-list read remained
   within 1.2%.  Graph construction, nested scheduling, and switch lifecycle
   improved by 4.2% to 6.7%, 8.5% to 9.6%, and 0.8% to 4.0%, while the window
   microbenchmarks were 7.0% to 17.1% slower.  The expected hot-path cost is
   one ``TypeRecord``-to-ops load in place of the removed cached cursor word.
   The review decision is whether the substantially smaller canonical
   representation justifies that measured read tradeoff.

Recommended model allocation
   A cost-effective model performs cleanup and matrix fixes.  The final audit
   and ABI decision belong to the highest-reasoning model and human reviewers.

Milestone 10: Performance Attribution And Window Recovery
----------------------------------------------------------

Purpose
   Turn the Milestone 9 timing samples into reproducible evidence and remove
   attributable window regressions without growing the common two-word erased
   pointer representation.

Implementation work
   Extend the standalone benchmark with distribution statistics and focused
   window setup, mutation, eviction, and read workloads.  Document a pinned-CPU
   Linux procedure, capture compiler and host identity with every result, and
   use profiling or generated assembly to identify repeated record-to-ops
   resolution inside hot loops.  Optimisations may cache resolved operations
   in operation-local or specialised mutation state, but must not add an ops
   word to ``AnyPtr``, typed pointers, or ``TSDataStorageRef``.

Acceptance
   The Ubuntu 24.04 VM produces repeatable before/after distributions with the
   same compiler and host configuration.  Window allocation counts remain
   zero, public erased-pointer layouts remain unchanged, and tick and duration
   steady-state workloads are within five percent of their accepted baseline
   unless profiling provides a reviewed explanation.  The full time-series
   and sanitizer suites pass.

Implemented
   Benchmark format 2 adds substring filtering, host/compiler/architecture
   labels, tenth and ninetieth percentiles, median absolute deviation, and
   diagnostic window cases which separate bound reads, view construction,
   mutation/eviction, and combined mutation/read.  The documented Linux
   procedure pins old and new binaries to the same virtual CPU and alternates
   them to expose VM drift.

   Compound window reads now resolve the canonical record's ops table once and
   reuse its layout, size, and storage projections.  A mutation scope validates
   mutability at construction, then inlines its small trusted helpers around
   the already-resolved ops table.  ``TSWDataView::begin_mutation`` uses a
   private trusted-storage constructor because the source specialised view has
   already checked the TSW kind; direct public construction retains the check.
   No ops pointer is stored in ``TSDataStorageRef`` or any public erased
   pointer, and their Milestone 9 layouts are unchanged.

   On the Ubuntu 24.04 x86-64 OrbStack VM with GCC 13.3, an immediate pinned
   21-sample comparison against ``99fd9957`` measured tick bound read, view
   read, mutation-only, and combined work at 10.536, 21.427, 80.274, and
   88.786 ns versus 14.380, 26.304, 86.357, and 101.603 ns.  Duration bound
   read, mutation-only, and combined work measured 9.458, 90.151, and 98.553
   ns versus 17.076, 93.951, and 111.271 ns.  Thus both combined paths are
   more than eleven percent faster than the pre-migration implementation and
   every diagnostic case is faster.  All reported window allocation and byte
   counts remain zero.

Milestone 11: Shared-Library ABI Boundary
------------------------------------------

Purpose
   Prove that the common records and compact pointers remain valid when their
   producer and consumer are compiled separately.

Implementation work
   Add a small shared-library fixture that exports canonical records and erased
   pointers to a separately compiled test executable.  Cover read-only,
   writable, typed-null, wrong-family, wrong-role, and unsupported ABI cases.
   Record size, alignment, standard-layout, and trivial-copy properties for
   public data-only ABI structures.  Exercise installed headers and the public
   ``hgraph::core`` CMake target rather than relying only on build-tree state.

Acceptance
   Boundary tests pass for shared and static C++ builds on macOS and Linux.
   Incompatible ABI versions fail before an ops call or data dereference.  The
   Python ``cp312-abi3`` wheel is audited and installed under every supported
   Python version.  Only the reviewed data-only record and descriptor fields
   are declared stable; C++ ownership classes remain outside the binary ABI.

Implemented
   ``hgraph_abi_boundary_producer`` is a shared fixture with a private
   ``TypeRecordRegistry`` and private pointer implementation.  Its separately
   compiled consumer checks ``SchemaHeader``, ``TypeRecord``, and ``AnyPtr``
   layout properties before exercising cross-library read, write, mutation,
   typed-null, family/role narrowing, ops dispatch, and unsupported-version
   rejection.  This prevents the test from passing merely because both sides
   share one in-process registry implementation.

   The fixture passes with static and shared ``hgraph::core`` builds on macOS
   and with a GCC 13.3 shared build on Linux.  A downstream project also
   configures against an installed shared package and links only the public
   ``hgraph::core`` target.  That test exposed and fixed list separators in
   the generated dependency fragment.  Shared builds now enable position-
   independent code for all linked static dependencies.  The existing wheel
   workflow audits the ``cp312-abi3`` wheel and installs it under Python 3.12,
   3.13, and 3.14 on each supported wheel platform.

Milestone 12: Native Platform CI
--------------------------------

Purpose
   Raise platform confidence from wheel compilation to native runtime
   execution while retaining Linux as the required performance platform.

Implementation work
   Add pure-C++ configure, build, and CTest jobs for current AppleClang, GCC,
   and MSVC.  Enable the repository warning policy on every compiler, with
   narrow documented exclusions only where a compiler warning is demonstrably
   incorrect.  Keep the existing Python 3.12 through 3.14 stable-ABI wheel
   matrix and run the Python bridge suite from the installed wheel.

Acceptance
   macOS and Linux native jobs are required.  Windows remains best effort, but
   it compiles with ``/W4 /permissive-`` and executes both the C++ and Python
   suites so failures contain runtime evidence rather than compile evidence
   alone.  Tagged releases may reuse artifacts only from a successful workflow
   for the exact commit.

Implemented
   The release workflow now configures a Python-disabled native C++ build on
   ``ubuntu-latest``, ``macos-26``, and ``windows-2025-vs2026``.  Linux and
   macOS use Ninja with their current native compiler; Windows explicitly uses
   the ``Visual Studio 18 2026`` generator so it cannot silently select MinGW.
   It builds and runs CTest with the repository warning policy enabled; Linux
   and macOS are required and Windows is explicitly best effort.  Python 3.12
   is present in this job only as build tooling to locate the Arrow libraries
   shipped by pyarrow.  Nanobind and Python development headers are not part
   of the native targets.

   ``HGRAPH_FETCH_SIMDJSON`` makes the hosted build's pinned simdjson explicit,
   while the default pure-C++ configure continues to use normal CMake package
   discovery.  A newly built release requires the native job as well as wheel
   and sdist validation.  A tag can still reuse artifacts only from a
   successful workflow with the exact commit SHA.  The existing installed-
   wheel matrix executes the Python compatibility suite on Python 3.12 through
   3.14, including Windows.

Milestone 13: Interactive Debugger Validation
----------------------------------------------

Purpose
   Validate the original debugging objective with realistic stopped-process
   structures rather than printer snapshot tests alone.

Implementation work
   Build deterministic debugger fixtures containing native and Python nodes,
   scalar and composite values, both switch banks, and live, stopped/deleted,
   and erased map/mesh slots.  Add command-driven GDB and LLDB smoke tests that
   inspect those fixtures without inferior calls.  Include malformed records,
   typed-null values, and unsupported descriptor versions to verify safe
   failure.

Acceptance
   A developer can navigate graph to node to endpoint to value using semantic
   and implementation labels without knowing private template spellings.
   Automated smoke tests match the debugger-independent decoder, and a manual
   review records the commands and representative output for both debuggers.

Implemented
   The stopped-process fixture now publishes scalar, bundle, list, mutable-map,
   node, and graph pointers together with typed-null, malformed-pointer,
   invalid-record, and invalid-descriptor cases.  LLDB and GDB batch commands
   assert semantic and implementation labels and navigate bundle fields, map
   key/value children, and graph-to-node children without calling inferior
   methods.  The LLDB gate passes on macOS and is enabled in native CI; the GDB
   gate is enabled on Linux CI.  Local GDB execution in the OrbStack VM is a
   known platform limitation because that VM denies ``ptrace`` register access,
   but the script loads and registers its command there.

   Expanding the fixture found two unreachable GDB reader bodies and corrected
   them.  LLDB synthetic children now retain the semantic names requested by
   the descriptor decoder.  The commands and expected success messages are
   recorded in :doc:`../debugging`.

Milestone 14: Adversarial Lifetime And Release Gate
---------------------------------------------------

Purpose
   Stress ownership and nested-slot state transitions before freezing the
   reviewed ABI fields.

Implementation work
   Add deterministic state-machine tests for construct, copy, move, start,
   stop, delete, erase, reuse, and construction failure.  Generate map and mesh
   key churn across capacity growth and delayed erase, and exercise repeated
   Python/native transitions.  Keep checked-in benchmark and layout baselines
   with thresholds that distinguish repeatable regressions from timer noise.

Acceptance
   ASan/UBSan pass the generated lifetime suite on macOS and Linux; periodic
   TSan runs cover shared registries and Python bridge entry points.  Nested
   steady state remains allocation-free, every stopped slot is destroyed once
   on erase, and release evidence contains layouts, allocation counts,
   benchmark distributions, compiler versions, and known platform limits.

Implemented
   A deterministic key-slot state machine mixes insert, duplicate insert,
   delayed remove, resurrection, erase, capacity growth, move round trips, and
   clear while an independent model verifies key ownership and every slot's
   constructed/live/pending state after each transition.  A Python test runs
   the same graph 32 times while alternating Python compute nodes and native
   operations, validating that bridge type and state lifetimes survive repeated
   run teardown.

   The complete 447-case C++ value suite passes under ASan and UBSan on macOS
   and on Ubuntu 24.04 with GCC 13.3; the Linux run contains 231,971 assertions.
   The same 447 cases and 231,977 assertions pass under AppleClang 21
   ThreadSanitizer, including the concurrent intern and registry tests.  A GCC
   13.3 TSan build is warning-clean, but the OrbStack VM aborts its runtime
   before ``main`` with an unsupported address-space mapping.  The focused
   Python/native repetition test also passes.  Window benchmarks report zero
   allocations and the exact compact pointer layout tests remain unchanged.
   Periodic Python-entry TSan coverage and richer Python-node debugger fixtures
   remain deferred until the native platform gates have accumulated stable CI
   evidence.

Milestone 15: Release Evidence And Compatibility Closure
--------------------------------------------------------

Purpose
   Turn the accepted type-erasure implementation into a repeatable release
   gate, and close the remaining nested-graph, installed-package, debugger, and
   measurement gaps without expanding the common pointer representation.

Implementation work
   Add a required Linux shared-library build that installs the CMake package
   and builds a separately configured downstream consumer.  Exercise real
   switch, map, and mesh instances in the debugger fixture and assert their
   exact construct/start/stop/destroy lifetimes.  Check in a format-two Linux
   benchmark baseline and a comparator which treats allocation counts, bytes,
   benchmark inventory, and checksums as exact while using a MAD-aware timing
   threshold.  Record an explicit ABI evolution policy for every frozen common
   record.

Acceptance
   Current AppleClang and GCC builds pass the complete native suite with
   warnings as errors; the shared Linux install and downstream
   ``find_package(hgraph)`` consumer pass independently of the source tree.
   Visual Studio 2026 compiles and executes the native suite as a best-effort
   check.  LLDB and GDB can descend from real switch banks and keyed map/mesh
   slots into their child graphs.  Exact lifecycle tests prove stop-on-delete,
   destruction-on-erase, and alternating switch-bank reuse.  The pinned Linux
   benchmark comparison reports exact allocation evidence and separates
   repeatable timing regressions from host noise.

Implemented
   The release workflow contains a required Python-disabled Linux shared build,
   full CTest run, install, and separately configured downstream consumer.  The
   Windows native job is pinned to ``windows-2025-vs2026`` and the ``Visual
   Studio 18 2026`` CMake generator; Ninja cannot select MinGW for that matrix
   entry.  The downstream consumer passes against the installed shared package
   and public ``hgraph::core`` target on GCC 13.3.

   The debugger fixture executes real switch, map, and mesh nodes.  It retains
   both switch banks and keyed live/pending slots at the breakpoint, and the
   LLDB/GDB validators descend into every nested graph.  This exposed and fixed
   map/mesh dynamic-layout offsets which previously described the enclosing
   storage component rather than its nested entry store.  Exact counter tests
   prove switch A/B/A placement reuse and map/mesh stop-on-delete followed by
   destruction-on-erase.  The new tests and fixture pass GCC ASan/UBSan.  The
   local Linux VM still cannot run the GDB subprocess because it denies
   ``ptrace``; the script loads there and remains enabled on hosted Linux CI.

   ``benchmarks/type_erasure/linux_gcc13_milestone15.txt`` is the accepted
   static Release baseline.  ``tools/compare_type_erasure_perf.py`` requires
   the same host, compiler, benchmark inventory, iterations, checksums,
   allocation counts, and allocated bytes.  Timing fails only when the current
   median exceeds both five percent and three times the larger measured MAD.
   A second pinned 21-sample run passed every comparison.  Steady window and
   nested scheduling cases allocate zero bytes; graph construction records 22
   allocations per graph and the switch lifecycle records 1,502.4 allocations
   per measured operation.  The reproducible command is:

   .. code-block:: console

      taskset -c 0 env \
        HGRAPH_TYPE_ERASURE_PERF_HOST=orbstack-ubuntu24-x86_64 \
        HGRAPH_TYPE_ERASURE_PERF_SAMPLES=21 \
        HGRAPH_TYPE_ERASURE_PERF_WARMUP=5000 \
        build/tests/cpp/hgraph_type_erasure_perf | \
        python3 tools/compare_type_erasure_perf.py \
          benchmarks/type_erasure/linux_gcc13_milestone15.txt -

   AppleClang 21 and GCC 13.3 are warning-clean.  The fresh macOS Release suite
   passes all 978 ordinary tests and its LLDB nested-navigation smoke test; the
   Linux shared suite passes the same ordinary tests and ABI boundary.  The
   ABI evolution rules now state exactly which common fields are frozen and
   which version and boundary fixtures change with each kind of evolution.

Review Evidence
---------------

Each milestone handoff should contain the following evidence in a consistent
form:

.. code-block:: text

   Milestone and iteration:
   Approved contract or design reference:
   Files changed:
   Behaviour changed:
   Layouts before and after:
   Tests added:
   Commands run and results:
   Benchmarks and allocation counts:
   Known limitations or deferred work:
   Unrelated worktree changes excluded:

This report is an index for review, not proof of correctness.  The review model
must still inspect the repository, diff, tests, and relevant generated behaviour.

Suggested Commit Boundaries
---------------------------

Commit only accepted work.  Milestones 0 through 3 can normally be one commit
each.  Time-series, node/graph, and debugger milestones should use one commit
per named iteration.  Pointer/owner separation and nested-graph validation
should remain separate even if they are developed together, because their
rollback and lifetime risks differ.

The sequence is intentionally conservative.  It spends the larger reasoning
budget at design and acceptance boundaries while assigning searches,
mechanical migration, exhaustive tests, printer implementation, and cleanup to
the more cost-effective model.  That division is useful only while milestones
remain narrow; a large instruction such as "migrate type erasure" should always
be split before delegation.
