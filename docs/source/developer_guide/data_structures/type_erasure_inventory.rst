Type Erasure Inventory And Baseline
===================================

Status
------

This chapter records the historical Milestone 0 implementation at commit
``ed0d40dce8edfbd8e6d0740f2f1261b7a853a9ff``.  It is an inventory of the
system as it existed at that commit, not the current API described in
:doc:`unified_type_erasure`.
The layout assertions and performance executable added with this inventory do
not change runtime behaviour.

Reproducing The Inventory
-------------------------

Run the searches from the repository root.  Restricting the file extensions
keeps generated files and documentation examples out of the results.

.. code-block:: console

   rg -n 'using [A-Za-z0-9_]*Binding\s*=\s*TypeBinding|TypeBinding<' include src tests/cpp --glob '*.{h,hpp,cpp}'
   rg -n 'using [A-Za-z0-9_]*StorageRef|StorageRef<' include src tests/cpp --glob '*.{h,hpp,cpp}'
   rg -n 'StorageHandle' include src tests/cpp --glob '*.{h,hpp,cpp}'
   rg -n '::intern\(|::find\(|::clear\(|binding_for\(|builder_for\(' include src --glob '*.{h,hpp,cpp}'
   rg -n 'type_meta|storage_plan|\.ops\b|->ops\b' include src --glob '*.{h,hpp,cpp}'
   rg -n 'static_cast<const .*Binding \*>\(context\)|as<const .*Binding>' include src --glob '*.{h,hpp,cpp}'
   rg -n 'static const .*TypeBinding|TypeBinding binding\{' include src --glob '*.{h,hpp,cpp}'
   rg -n 'PrettyPrinter|pretty|TypeBinding|StorageRef|ValueView|TSDataView' tools/debugger --glob '*.py'

The first search produces six production ``TypeBinding`` aliases.  A direct
use of ``TypeBinding<TSValueTypeMetaData, TSDataOps>::find`` in
``src/hgraph/types/time_series/ts_input.cpp`` is the same specialization as
``TSDataBinding``, not a seventh binding family.  Test-local binding types in
``test_memory_utils.cpp`` are generic memory utility fixtures and are not
runtime families.

Common Physical Model
---------------------

``TypeBinding<Schema, Ops>`` is a standard-layout, trivially-copyable triple:
``const Schema *type_meta``, ``const StoragePlan *storage_plan``, and
``const Ops *ops``.  Its per-template ``InternTable`` interns by the complete
pointer triple.  The common shape is not a common runtime ABI: only the C++
template specialization says how to interpret the schema and ops pointers.

``MemoryUtils::StorageRef<Binding>`` is a borrowed two-word cursor containing
``const Binding *`` and ``void *``.  It never constructs, destroys, or owns the
payload.  ``MemoryUtils::StorageHandle<Policy, Binding>`` is currently three
words and can be owning-inline, owning-heap, borrowed, typed-empty, or
unbound.  Its ``m_identity`` word is a plan pointer for ``Binding=void`` and a
binding pointer otherwise.

``TSDataStorageRef<DataOps>`` adds a cached ``const DataOps *`` to a two-word
``StorageRef<TSDataBinding>``.  Both the generic
``TSDataStorageRef<TSDataOps>`` and the specialized forms are therefore three
words.  ``TSDataView`` contains the generic form and is also three words.  The
cache avoids repeatedly selecting or downcasting the specialized ops table,
but it is an exception to the desired two-word pointer model.

The complete current-layout baseline is enforced by
``tests/cpp/test_type_erasure_layout.cpp``:

.. list-table:: Current erased layout baseline
   :header-rows: 1
   :widths: 42 18 40

   * - Type group
     - Pointer words
     - Assertion
   * - All six ``TypeBinding`` specializations
     - 3
     - Standard layout, trivially copyable, pointer aligned.
   * - ``StorageRef`` for all six bindings
     - 2
     - Standard layout, trivially copyable, pointer aligned.
   * - ``ValueView``, ``NodeView``, ``GraphView``, ``GraphExecutorView``
     - 2
     - Binding/data cursor, with value access encoded in binding tag bits.
   * - ``EvaluationClockView``
     - 2
     - Wraps ``EvaluationClockStorageRef``.
   * - Generic and specialized ``TSDataStorageRef``
     - 3
     - Binding, data, and cached ops.
   * - ``TSDataView``
     - 3
     - Contains generic ``TSDataStorageRef``.
   * - Raw-plan and binding-specialized ``StorageHandle``
     - 3
     - Identity, tagged allocator/state, and storage word.

The six explicit ``StorageRef`` instantiations have these production surfaces:

.. list-table:: Complete StorageRef specialization inventory
   :header-rows: 1
   :widths: 34 25 41

   * - Specialization
     - Named alias
     - Current use
   * - ``StorageRef<ValueTypeBinding>``
     - None
     - The generic two-word shape is valid and baseline-tested, but
       ``ValueView`` currently implements its own tagged binding/data pair.
   * - ``StorageRef<TSDataBinding>``
     - None
     - Private first two words of every ``TSDataStorageRef<DataOps>``.
   * - ``StorageRef<NodeTypeBinding>``
     - ``NodeStorageRef``
     - ``NodeView``, graph/node parent links and nested graph construction.
   * - ``StorageRef<GraphTypeBinding>``
     - ``GraphStorageRef``
     - ``GraphView`` and graph-backed injectables.
   * - ``StorageRef<GraphExecutorTypeBinding>``
     - ``GraphExecutorStorageRef``
     - ``GraphExecutorView`` and root-graph parent identity.
   * - ``StorageRef<EvaluationClockTypeBinding>``
     - ``EvaluationClockStorageRef``
     - ``EvaluationClockView`` and node clock injection.

Before this milestone, ``test_memory_utils.cpp`` asserted the default
``StorageHandle`` size but there was no single test locking all production
bindings, refs, and core views together.  The new test intentionally does not
lock endpoint-view sizes, specialized value/TS view sizes, owner class sizes,
ops-table sizes, or schema sizes.  Those structures carry family state rather
than the proposed compact pointer ABI and may legitimately change.  It also
does not claim a stable cross-release binary ABI; it detects accidental layout
changes within the migration.

Schemas, Kinds, And Ops ABIs
----------------------------

Only value and time-series schemas currently share ``TypeMetaData`` and its
``MetaCategory`` discriminator.  The other four schema structures are
unrelated C++ types.  Every family has a narrow ops ABI; there is no common ops
prefix and no runtime family discriminator on a binding.

.. list-table:: Current schema and ops families
   :header-rows: 1
   :widths: 13 22 25 24 16

   * - Family
     - Schema
     - Current kinds or modes
     - Ops ABI
     - Current classification
   * - Value
     - ``ValueTypeMetaData``
     - ``Atomic``, ``Tuple``, ``Bundle``, ``List``, ``Set``, ``Map``,
       ``CyclicBuffer``, ``Queue``, ``Any``
     - ``ValueOps``; derived ``IndexedValueOps``, list, set, map, cyclic
       buffer, and queue tables
     - Schema, Ops
   * - Time series
     - ``TSValueTypeMetaData``
     - ``TS``, ``TSS``, ``TSD``, ``TSL``, ``TSW``, ``TSB``, ``REF``,
       ``SIGNAL``
     - ``TSDataOps``; derived ``TSSDataOps``, ``TSDDataOps``,
       ``IndexedTSDataOps``, and ``TSWDataOps``
     - Schema, Ops
   * - Node
     - ``NodeTypeMetaData``
     - ``Compute``, ``PushSource``, ``PullSource``, ``Sink``, ``Nested``
     - ``NodeOps`` plus optional extended-view identity/context
     - Schema, Ops
   * - Graph
     - ``GraphTypeMetaData``
     - No schema kind; ``Root``/``Nested`` currently lives in ``GraphOps``
     - ``GraphOps``
     - Schema, Ops
   * - Executor
     - ``GraphExecutorTypeMetaData``
     - ``Simulation``, ``RealTime`` mode
     - ``GraphExecutorOps``
     - Schema/config, Ops
   * - Clock
     - ``EvaluationClockTypeMetaData``
     - No kind
     - ``EvaluationClockOps``
     - Schema, Ops

The derived value and TS ops tables are implementation refinements, not
additional binding specializations.  A ``ValueTypeBinding`` always stores a
``ValueOps *`` and a ``TSDataBinding`` always stores a ``TSDataOps *``; code
selects or casts the richer ABI only after inspecting schema kind or
``TSDataOps::kind``.

Binding Specializations
-----------------------

.. list-table:: Complete production binding inventory
   :header-rows: 1
   :widths: 18 21 19 22 20

   * - Alias and definition
     - Schema and Ops
     - Creation/interning
     - Principal consumers
     - Migration note
   * - ``ValueTypeBinding`` in ``value_ops.h``
     - ``ValueTypeMetaData`` / ``ValueOps``
     - ``TypeRegistry`` for atomics; ``ValuePlanFactory`` and container
       factories for composites and representations
     - ``Value``, ``ValueView``, value builders and projected TS values
     - Value-family pilot; preserve owning-binding remapping for projected
       views.
   * - ``TSDataBinding`` in ``ts_data/types.h``
     - ``TSValueTypeMetaData`` / ``TSDataOps``
     - ``TSDataPlanFactory`` plus fixed, dynamic-list, slot, window, proxy,
       target-link, and mapped-key-source implementations
     - ``TSData``, all TS data/input/output views, nodes and graph links
     - Data/Input/Output roles require a human decision; existing input and
       output endpoints reuse data bindings.
   * - ``NodeTypeBinding`` in ``node_fwd.h``
     - ``NodeTypeMetaData`` / ``NodeOps``
     - ``NodeRuntimeRegistry`` in ``node.cpp``
     - ``NodeValue``, ``NodeView``, graph node entries and TS parent links
     - Extended views must narrow from the common Node family without adding
       pointer words.
   * - ``GraphTypeBinding`` in ``graph.h``
     - ``GraphTypeMetaData`` / ``GraphOps``
     - ``GraphRuntimeRegistry`` creates separate root and nested bindings
     - ``GraphValue``, ``GraphView``, graph executor and nested nodes
     - Whether Root/Nested is a role or implementation property is deferred
       to the Milestone 1 gate.
   * - ``GraphExecutorTypeBinding`` in ``graph.h``
     - ``GraphExecutorTypeMetaData`` / ``GraphExecutorOps``
     - ``ExecutorRuntimeRegistry`` in ``executor.cpp``
     - ``GraphExecutorValue/View``, root graph parent cursor
     - Likely Runtime role; mode remains semantic/configuration data.
   * - ``EvaluationClockTypeBinding`` in ``evaluation_clock.h``
     - ``EvaluationClockTypeMetaData`` / ``EvaluationClockOps``
     - Direct static default/simulation/realtime/mock bindings
     - ``EvaluationClockView`` and executor injection
     - Static records must become canonical records or an explicitly valid
       immortal-record construction path.

Pointers, Views, Owners, And Builders
-------------------------------------

.. list-table:: Current object classification and ownership
   :header-rows: 1
   :widths: 24 15 26 35

   * - Current object
     - Proposed term
     - Ownership
     - Construction/interning path
   * - ``TypeBinding<Schema, Ops>``
     - Type Record
     - Immortal/test-reset interned metadata; direct statics also exist
     - Per-specialization ``InternTable`` keyed by schema/plan/ops.
   * - ``StorageRef<Binding>``
     - Pointer
     - Borrowed
     - Made from a binding and data address by views and parent links.
   * - ``TSDataStorageRef``
     - Specialized Pointer
     - Borrowed
     - Adds cached ops selected from a ``TSDataBinding``.
   * - ``ValueView`` and specialized value views
     - View
     - Borrowed
     - Wrap binding/data; access mode is tagged on the aligned binding pointer.
   * - ``TSDataView`` and shape views
     - View
     - Borrowed
     - Wrap ``TSDataStorageRef``; shape views retain specialized cached ops.
   * - ``TSInputView`` / ``TSOutputView``
     - Endpoint View
     - Borrowed
     - Add endpoint identity, target-link state and/or evaluation time around
       TS data views; they are not compact generic pointers.
   * - ``NodeView``, ``GraphView``, ``GraphExecutorView``, clock view
     - View
     - Borrowed
     - Wrap the matching two-word ``StorageRef``.
   * - ``Value``, ``TSData``, ``NodeValue``, ``GraphValue``, executor value
     - Owner
     - Usually owning through binding-specialized ``StorageHandle``
     - Construct through family builders/factories and bound plan lifecycle.
   * - ``GraphValue`` using external nested-graph storage
     - Pointer disguised as Owner
     - Borrowed ``StorageHandle`` state; ``external_payload_`` records it
     - ``storage_type::reference`` in ``graph.cpp``.  This is a primary
       Milestone 7 split site.
   * - ``Value::reference``
     - Pointer disguised as Owner
     - Borrowed ``StorageHandle`` state
     - Used to move a child out of externally-owned parent storage.  Also a
       primary Milestone 7 split site.
   * - Dynamic ``TSL`` element handles
     - Owner
     - Heap-only binding-specialized ``StorageHandle`` per stable child
     - ``ts_data_dynamic_list_ops.cpp``; preserve stable child addresses.
   * - ``Value`` container builders, ``NodeBuilder``, ``GraphBuilder``,
       ``GraphExecutorBuilder``, ``TSInputBuilder``
     - Builder
     - Own recipes/configuration, not runtime payload
     - Reusable front ends to plans, bindings, and construction.

``TSOutputHandle`` is not a type record or storage owner.  It is a stable
endpoint cursor consisting of output identity plus a three-word TS data
cursor.  ``TimeSeriesReference`` is application-level REF data, not the new
generic pointer concept.  These overloaded uses of *handle* and *reference*
must not be mechanically renamed without preserving their endpoint semantics.

Factories, Registries, And Reset
--------------------------------

.. list-table:: Current interning and factory layers
   :header-rows: 1
   :widths: 25 38 37

   * - Component
     - Stable objects/caches
     - Reset and migration concern
   * - ``TypeRegistry``
     - Value and TS schemas, interned names, scalar bindings
     - Reset last because every other layer borrows schemas.
   * - ``MemoryUtils`` plan registries
     - Scalar, composite, array, and other storage plans
     - Plans are canonical physical layout/lifecycle recipes; do not merge
       them with schemas.
   * - ``ValuePlanFactory``
     - Schema-to-plan and schema-to-default-binding caches
     - Supports multiple non-default bindings for projected/compact/mutable
       representations outside the one default cache.
   * - ``TSDataPlanFactory``
     - TS-schema-to-data-plan and default-data-binding caches
     - Additional specialized bindings are created by representation modules.
   * - ``TSInputBuilderFactory``
     - Endpoint-schema construction plans, builders, target-link contexts
     - Input representation is currently outside ``TypeBinding`` identity.
   * - ``NodeRuntimeRegistry``
     - Node schemas, plans, callbacks/contexts, ops and bindings
     - Private registry in ``node.cpp``; stable storage supplies ops contexts.
   * - ``GraphRuntimeRegistry``
     - Graph schemas and root/nested implementation entries/bindings
     - Root/nested currently select different plan/ops representations.
   * - ``ExecutorRuntimeRegistry``
     - Executor schemas, plan/ops implementations and bindings
     - Separate simulation and real-time implementations.
   * - ``TypeBinding`` registry
     - One intern table for each ``<Schema, Ops>`` specialization
     - Current identity is fragmented by C++ type; target registry is common.

``reset_all_registries`` implements borrower-before-lender teardown.  It
currently clears operator/converter caches, value and TS plan factories,
input builders, compact plans, ``TSDataBinding`` and ``ValueTypeBinding``, then
resets ``TypeRegistry``.  Node, graph, executor, and clock bindings are not
cleared by this path: runtime registries and direct static bindings have
process lifetime.  Milestone 1 must state explicitly whether the common
type-record registry is process-lifetime or test-resettable before it can own
all six families.

Direct And Static Bindings
--------------------------

Not every binding is returned by ``TypeBinding::intern``:

* default typed-null bindings for ``NodeView``, ``GraphView``,
  ``GraphExecutorView`` and ``EvaluationClockView`` are direct function-local
  statics;
* simulation and real-time evaluation-clock bindings in ``executor.cpp`` are
  direct statics over the executor storage plans;
* ``MockGraphExecutor`` supplies direct test-only clock and executor bindings;
* runtime-created node, graph and executor bindings are interned after their
  private registries establish stable schema, plan and ops storage.

The common registry must either intern the default/static records or provide
one checked immortal-record mechanism.  Leaving direct records with no common
magic/ABI header would recreate the debugger ambiguity.

Concrete Binding Dependencies
-----------------------------

Current code depends on concrete binding C++ types in four ways:

1. Views store ``StorageRef<ConcreteBinding>`` and expose concrete
   ``binding()`` return types.  Public callbacks also mention these types, for
   example value container element-binding callbacks and TS data child-binding
   callbacks.
2. Hot paths directly read ``type_meta``, ``storage_plan`` and ``ops``.  The
   reproducible search above finds these throughout value builders, container
   ops, TS data representations, node/graph/executor implementations and
   wiring.
3. Ops ``context`` pointers sometimes carry implementation contexts and
   sometimes carry concrete binding pointers.  Explicit examples include the
   Value-binding casts in ``ts_data_slot_ops.cpp`` and ``mesh_node.cpp``, and
   the TS-binding casts in window projection callbacks.
4. Parent and endpoint links use tagged pointers whose pointed-to C++ type is
   selected by a separate enum, then recover ``TSDataBinding`` or
   ``NodeTypeBinding`` with ``tagged_ptr::as<T>``.

These are migration sites, not necessarily defects.  Family-typed pointer
wrappers must keep narrow callback signatures ergonomic while widening to the
same common record.  Ops contexts must be inventoried per implementation
before assuming they point at a schema, binding, layout, or arbitrary context.

Debugger Entry Points
---------------------

``tools/debugger/hgraph_gdb.py`` and ``hgraph_lldb.py`` register printers and
synthetic child providers for exactly these types:

* ``Value`` and ``ValueView``;
* ``TypeMetaData``, ``ValueTypeMetaData``, and ``TSValueTypeMetaData``;
* every ``TypeBinding<...>`` specialization through one regular expression;
* ``MemoryUtils::StoragePlan``;
* ``TSDataView``, ``TSInputView``, and ``TSOutputView``.

There are no registered printers for TS owners such as ``TSData``, ``TSInput``
or ``TSOutput``.  There are also no ``NodeView``/``NodeValue``,
``GraphView``/``GraphValue``, ``GraphExecutorView``/``GraphExecutorValue``, or
``EvaluationClockView`` printers.  Both debugger scripts currently register
the same ten entry-point patterns.  They dispatch with C++ type-name regular
expressions and contain explicit spellings for value and TS binding
specializations.  Deep display duplicates ``ValueTypeKind`` and
``TSTypeKind`` knowledge and reads private fields such as the tagged binding
bits and TS cached ops pointer.

There is no reliable entry point for an arbitrary binding address: the printer
must already know whether the first pointer is a value schema, TS schema, node
schema, graph schema, executor schema, or clock schema.  A shallow universal
printer is therefore impossible with the current memory ABI.  This is the
baseline problem Milestones 1 and 6 must solve; the current printers should not
gain more guessed casts in the meantime.

Provisional Target Classification
---------------------------------

This table guides the first implementation contract.  Entries marked
*provisional* are human review gates, not enum values committed by Milestone 0.

.. list-table:: Provisional family, role, and kind mapping
   :header-rows: 1
   :widths: 18 25 32 25

   * - Family
     - Roles
     - Kinds
     - Decision status
   * - Value
     - ``Instance``
     - Existing ``ValueTypeKind`` values
     - Recommended.
   * - TimeSeries
     - ``Data``; ``Input`` and ``Output`` provisional
     - Existing ``TSTypeKind`` values
     - Input/output currently compose or project TSData rather than carrying
       independent bindings.  Decide after modeling endpoint storage.
   * - Node
     - ``Runtime``
     - Existing ``NodeKind`` values
     - Recommended; extended node views are capabilities, not kinds.
   * - Graph
     - ``Runtime`` or provisional ``Root``/``Nested`` roles
     - No separate semantic kind today
     - Human gate: parent relationship is currently an implementation choice
       in ``GraphOps`` and affects layout.
   * - Executor
     - ``Runtime``
     - Simulation/RealTime may remain schema mode rather than common kind
     - Recommended family; kind representation to confirm.
   * - Clock
     - ``Runtime`` or ``Projection``
     - No kind
     - Recommended family; clocks project executor storage.

Milestone 1 must not encode Root/Nested or Input/Output until the review
resolves whether each distinction selects a type record, is endpoint state, or
is only an implementation label/capability.  The first common records can be
validated with mock schemas without answering those runtime-family questions.

Performance Baseline
--------------------

``hgraph_type_erasure_perf`` is a standalone executable and is deliberately
not registered with CTest.  It uses ``steady_clock`` and overrides the C++
``new``/``delete`` forms in this standalone executable to count normal, array,
sized, aligned and nothrow allocations.  This intercepts calls resolved to
those overrides in the default statically-linked benchmark.  It does not count
direct ``malloc`` calls or allocations made through unrelated external
allocators, and a Windows shared build is not guaranteed to route allocations
inside DLLs through the executable's overrides.  Each case performs a
correctness check, warmup, and at least seven samples, then emits one
machine-readable line containing median, minimum, maximum, tenth and ninetieth
percentile, median absolute deviation, allocation/byte counts and a checksum.
The header identifies the host label, compiler family and version, and target
architecture.  It never fails on a timing threshold.

The cases establish these migration comparisons:

* atomic ``ValueView`` read;
* scalar TS current-value read;
* fixed composite TS child read;
* dynamic-list read and construction/growth;
* tick and duration window bound reads, view construction/read, mutation and
  eviction, and combined mutation/read;
* erased native ``NodeView::evaluate`` with node construction excluded;
* repeated small graph construction/destruction from one reusable builder;
* an allocation-free steady scheduled scan of map, mesh, and reduce nodes;
* an alternating ``switch_`` run which constructs, stops, retains and destroys
  nested branch graphs through the normal runtime lifecycle.

Use ``HGRAPH_TYPE_ERASURE_PERF_SAMPLES`` (minimum seven),
``HGRAPH_TYPE_ERASURE_PERF_WARMUP`` and
``HGRAPH_TYPE_ERASURE_PERF_ITERATIONS`` to control a run.  Use
``HGRAPH_TYPE_ERASURE_PERF_FILTER`` to select cases whose names contain one
substring and ``HGRAPH_TYPE_ERASURE_PERF_HOST`` to attach a whitespace-free
machine label.  The global iteration override is intended for smoke checks;
omit it when recording comparative baselines because the cases have
workload-appropriate defaults.

.. code-block:: console

   cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
   cmake --build build --target hgraph_unit_tests_value hgraph_type_erasure_perf
   build/tests/cpp/hgraph_unit_tests_value "current type-erasure records retain their baseline layouts"
   build/tests/cpp/hgraph_type_erasure_perf

Reproducible Linux comparison
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Linux performance evidence is recorded in the Ubuntu 24.04 OrbStack VM, not
on a changing hosted CI runner.  Build both commits with the same GCC Release
configuration.  Before each comparison record ``uname -a``, the relevant
``lscpu`` output, ``c++ --version``, and the commit ids.  Pin each process to
the same virtual CPU and run the old and new binaries alternately when more
than one comparison cycle is required:

.. code-block:: console

   uname -a
   lscpu
   c++ --version
   taskset -c 0 env \
       HGRAPH_TYPE_ERASURE_PERF_HOST=orbstack-ubuntu24-x86_64 \
       HGRAPH_TYPE_ERASURE_PERF_FILTER=tsw_ \
       HGRAPH_TYPE_ERASURE_PERF_SAMPLES=21 \
       HGRAPH_TYPE_ERASURE_PERF_WARMUP=5000 \
       build/tests/cpp/hgraph_type_erasure_perf

Report the median and relative median absolute deviation together.  Treat a
sub-five-percent change as unresolved timer or host variation unless repeated
cycles or a profiler attribute it to a specific path.  Allocation-count or
layout changes are exact and are reviewed independently of timing noise.

Provisional macOS baseline
~~~~~~~~~~~~~~~~~~~~~~~~~~

The independent Milestone 0 verifier recorded the following initial baseline
at starting commit ``ed0d40dce8edfbd8e6d0740f2f1261b7a853a9ff``.  The host
was macOS Darwin 25.5.0 arm64 with AppleClang 21.0.0 and CMake 4.4.0.  It used
the Release, default statically-linked pure-C++ build and no benchmark
environment overrides.  The exact command was:

.. code-block:: console

   /tmp/hg_cpp-te-m0-release/tests/cpp/hgraph_type_erasure_perf

The raw machine-readable output was:

.. code-block:: text

   type_erasure_perf format=1 samples=7 warmup_iterations=64
   benchmark name=atomic_value_read samples=7 iterations=200000 median_ns_per_op=0.495 min_ns_per_op=0.495 max_ns_per_op=0.497 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=8200000
   benchmark name=scalar_ts_read samples=7 iterations=100000 median_ns_per_op=4.205 min_ns_per_op=4.204 max_ns_per_op=4.205 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=4300000
   benchmark name=fixed_composite_ts_child_read samples=7 iterations=50000 median_ns_per_op=22.517 min_ns_per_op=22.505 max_ns_per_op=23.349 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=2350000
   benchmark name=erased_native_node_evaluate samples=7 iterations=100000 median_ns_per_op=5.139 min_ns_per_op=5.035 max_ns_per_op=5.422 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=100000
   benchmark name=small_graph_construct_destroy samples=7 iterations=2000 median_ns_per_op=1047.917 min_ns_per_op=1027.479 max_ns_per_op=1139.917 median_allocations=60126 median_allocations_per_op=30.063 median_bytes=7239168 median_bytes_per_op=3619.584 checksum=2000
   benchmark name=alternating_switch_nested_graph_lifecycle samples=7 iterations=20 median_ns_per_op=163197.900 min_ns_per_op=160025.000 max_ns_per_op=165681.250 median_allocations=24064 median_allocations_per_op=1203.200 median_bytes=3120900 median_bytes_per_op=156045.000 checksum=520

Canonical Linux/GCC local-VM baseline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Linux performance evidence is recorded in the local OrbStack VM rather than
CI.  This gives later milestones a stable host and VM configuration for
comparisons.  The VM was Ubuntu 24.04.4 LTS, Linux
7.0.11-orbstack-00360-gc9bc4d96ac70 x86_64, with GCC 13.3.0 and CMake 3.28.3.
The dependency set included simdjson 4.6.4 from the repository-pinned release
and Apache Arrow 25.0.0.  The build was Release, warnings-as-errors, default
statically-linked pure C++, with no benchmark environment overrides, at
starting commit ``ed0d40dce8edfbd8e6d0740f2f1261b7a853a9ff``.

.. code-block:: console

   cmake -S /Users/hhenson/CLionProjects/hg_cpp \
     -B /tmp/hg_cpp-te-m0-linux-v2 -GNinja \
     -DCMAKE_BUILD_TYPE=Release \
     -DCMAKE_PREFIX_PATH=/tmp/hg_cpp-te-m0-deps \
     -DBUILD_TESTING=ON \
     -DHGRAPH_BUILD_PYTHON_BINDINGS=OFF \
     -DHGRAPH_ENABLE_PYTHON_USER_NODES=OFF \
     -DHGRAPH_WARNINGS_AS_ERRORS=ON
   cmake --build /tmp/hg_cpp-te-m0-linux-v2 \
     --target hgraph_unit_tests_value hgraph_type_erasure_perf --parallel 4
   /tmp/hg_cpp-te-m0-linux-v2/tests/cpp/hgraph_unit_tests_value \
     "current type-erasure records retain their baseline layouts"
   /tmp/hg_cpp-te-m0-linux-v2/tests/cpp/hgraph_type_erasure_perf

The focused layout assertion passed.  Two consecutive benchmark invocations
produced identical allocation and byte counts; their timing medians differed
by at most 3.1 percent.  The second invocation is the canonical raw baseline:

.. code-block:: text

   type_erasure_perf format=1 samples=7 warmup_iterations=64
   benchmark name=atomic_value_read samples=7 iterations=200000 median_ns_per_op=1.886 min_ns_per_op=1.825 max_ns_per_op=1.948 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=8200000
   benchmark name=scalar_ts_read samples=7 iterations=100000 median_ns_per_op=10.227 min_ns_per_op=10.164 max_ns_per_op=10.412 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=4300000
   benchmark name=fixed_composite_ts_child_read samples=7 iterations=50000 median_ns_per_op=59.258 min_ns_per_op=58.933 max_ns_per_op=60.327 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=2350000
   benchmark name=erased_native_node_evaluate samples=7 iterations=100000 median_ns_per_op=12.982 min_ns_per_op=12.485 max_ns_per_op=13.253 median_allocations=0 median_allocations_per_op=0.000 median_bytes=0 median_bytes_per_op=0.000 checksum=100000
   benchmark name=small_graph_construct_destroy samples=7 iterations=2000 median_ns_per_op=1486.081 min_ns_per_op=1451.289 max_ns_per_op=1531.622 median_allocations=66002 median_allocations_per_op=33.001 median_bytes=7559256 median_bytes_per_op=3779.628 checksum=2000
   benchmark name=alternating_switch_nested_graph_lifecycle samples=7 iterations=20 median_ns_per_op=164241.400 min_ns_per_op=161012.200 max_ns_per_op=188901.700 median_allocations=25205 median_allocations_per_op=1260.250 median_bytes=2822912 median_bytes_per_op=141145.600 checksum=520

The broader Linux run completed 833 of 838 CTest cases successfully.  The five
failures were existing adaptor/service wiring order behaviours, outside the
Milestone 0 layout and benchmark changes.  Building the default ``all`` target
with warnings-as-errors also exposes GCC's ``-Wmismatched-new-delete`` in the
pre-existing ``hgraph_json_perf`` allocation interceptor; the dedicated
``hgraph_type_erasure_perf`` target builds cleanly under the same flags.  These
observations are recorded so they are not mistaken for type-erasure migration
regressions.

The local Linux VM is the canonical performance comparison host for later
milestones.  macOS results are supplementary cross-compiler evidence.  Windows
remains a best-effort compile, layout, and functional-test platform and is not
a performance or milestone progression gate.  Platform records must include
compiler, build type, architecture, commit, environment overrides, exact
command and complete output; timings from one host must not be treated as
thresholds for another.

Milestone 1 Human Gates
-----------------------

Before common records are connected to production families, review must decide:

* whether a graph's Root/Nested distinction is a role, implementation label,
  capability, or ordinary runtime relationship;
* whether TimeSeries Input/Output receive records of their own or remain
  endpoint objects around Data pointers;
* whether all common records are process-lifetime, or how a test reset prevents
  dangling records across all six families;
* how direct default and clock bindings enter the canonical registry;
* whether the first record adapter can preserve derived ops access without the
  third cached TS data pointer, or whether that optimization waits for the TS
  migration;
* which concrete-binding callback signatures remain typed facades and which
  should accept the common pointer.

No production enum, type record, pointer wrapper, or ownership change is part
of this inventory.
