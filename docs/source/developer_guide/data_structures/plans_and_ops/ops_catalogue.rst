Ops Catalogue
=============

.. admonition:: Status

   Authoritative **current-state reference** for every erased ops family in
   the runtime: which tables exist, how they relate, and which concrete
   implementation installs which table. The *rationale* for this shape is
   recorded in :doc:`../unified_type_erasure`; the pre-migration snapshot is
   preserved in :doc:`../type_erasure_inventory` (historical). When code and
   this page disagree, one of them is wrong — fix it in the same change.

This page answers three questions the rest of the chapter assumes:

1. What ops tables exist, per family, and what anchors them?
2. Which implementation specialisation installs which table?
3. What are the dispatch chains a read or write actually walks?

The anchor: ``TypeRecord``
--------------------------

Every erased type in the runtime — value, time-series, node, graph,
executor, clock — is identified by one interned, immutable
``TypeRecord`` (``include/hgraph/types/metadata/type_record.h``):

.. code-block:: cpp

   struct TypeRecord
   {
       std::uint32_t          magic;                // TYPE_RECORD_MAGIC
       std::uint16_t          abi_version;          // record layout ABI
       TypeRole               role;                 // Instance/Data/Runtime/Input/Output
       std::uint16_t          ops_abi_version;      // family ops-table ABI (ledger below)
       TypeCapabilities       capabilities;         // derived once at interning
       const char            *implementation_label; // participates in identity
       const SchemaHeader    *schema;               // family + kind + introspection
       const MemoryUtils::StoragePlan *plan;        // layout + LifecycleOps
       const void            *ops;                  // family-specific ops table
       const DebugDescriptor *debug;                // cold-path descriptor
       mutable const TypeRecord *external_value_owner; // published once via CAS
   };

Records are interned in the process-wide ``TypeRecordRegistry`` keyed by
``(schema, role, plan, ops, debug, implementation_label)``; interning the
same key with conflicting capabilities or ABI versions throws. Registry
storage never relocates, so a ``TypeRecord*`` is process-stable.

Each family wraps the record pointer in a one-word, role-checked *TypeRef*
(``ValueTypeRef``, ``BasicTSTypeRef<TypeRole>`` and its aliases
``TSDataTypeRef`` / ``TSInputTypeRef`` / ``TSOutputTypeRef``,
``NodeTypeRef``, ``GraphTypeRef``, ``ExecutorTypeRef``, ``ClockTypeRef``),
and each views live memory through a two-word typed pointer
(``ValuePtr``, ``TSDataPtr``, …) whose low two bits of the record pointer
carry the :ref:`access mode <ops-catalogue-access-modes>`.

.. mermaid::

   flowchart LR
      subgraph interned["Interned (TypeRecordRegistry, process-stable)"]
         TR["TypeRecord<br/>role · capabilities · ops_abi_version<br/>implementation_label"]
         SH["SchemaHeader<br/>family · kind · label"]
         SP["StoragePlan<br/>size · align · offsets<br/>LifecycleOps"]
         OPS["family ops table<br/>(ValueOps / TSDataOps / NodeOps / …)"]
         DBG["DebugDescriptor<br/>cold-path fields"]
         TR --> SH
         TR --> SP
         TR --> OPS
         TR --> DBG
      end
      REF["family TypeRef<br/>(one word: TypeRecord*)"] --> TR
      PTR["typed pointer<br/>(two words: tagged TypeRecord* + data*)"] --> TR
      PTR --> MEM["live instance memory<br/>(NOT interned)"]
      VIEW["family View<br/>(wraps the typed pointer)"] --> PTR

The six families
----------------

``TypeFamily`` enumerates ``Value``, ``TimeSeries``, ``Node``, ``Graph``,
``Executor``, ``Clock``. ``TypeRole`` distinguishes ``Instance``,
``Data``, ``Runtime``, ``Input``, ``Output`` — the TimeSeries family is
the only one interning the same schema under three roles (Data, Input,
Output).

.. list-table::
   :header-rows: 1
   :widths: 12 22 18 24 24

   * - Family
     - Ops struct (root)
     - TypeRef
     - Schema type behind ``SchemaHeader``
     - Primary factory / intern site
   * - Value
     - ``ValueOps``
     - ``ValueTypeRef``
     - ``ValueTypeMetaData``
     - ``ValuePlanFactory::synthesise_type`` → ``intern_value_type``
   * - TimeSeries
     - ``TSDataOps``
     - ``BasicTSTypeRef<Role>``
     - ``TSValueTypeMetaData``
     - ``TSDataPlanFactory`` per-kind factories → ``intern_ts_type``
   * - Node
     - ``NodeOps``
     - ``NodeTypeRef``
     - node runtime schema (``runtime/node.h``)
     - node runtime-type interning (``runtime/node_type_ref.h``)
   * - Graph
     - ``GraphOps``
     - ``GraphTypeRef``
     - graph runtime schema (``runtime/graph.h``)
     - graph runtime-type interning (``runtime/graph_type_ref.h``)
   * - Executor
     - ``GraphExecutorOps``
     - ``ExecutorTypeRef``
     - executor schema (``runtime/executor.h``)
     - executor interning (``runtime/executor_type_ref.h``)
   * - Clock
     - ``EvaluationClockOps``
     - ``ClockTypeRef``
     - clock schema
     - clock interning (``runtime/clock_type_ref.h``)

Ops-ABI ledger
--------------

Every record stamps the ABI version of the ops table it points at;
record validation rejects mismatches at interning time. Bump the
constant when the ops struct layout changes.

.. list-table::
   :header-rows: 1
   :widths: 30 12 58

   * - Constant
     - Value
     - Declared in
   * - ``VALUE_OPS_ABI_VERSION``
     - 6
     - ``include/hgraph/types/value/value_ops.h``
   * - ``TS_DATA_OPS_ABI_VERSION``
     - 10
     - ``include/hgraph/types/time_series/ts_type_ref.h``
   * - ``NODE_OPS_ABI_VERSION``
     - 4
     - ``include/hgraph/runtime/node_type_ref.h``
   * - ``GRAPH_OPS_ABI_VERSION``
     - 7
     - ``include/hgraph/runtime/graph_type_ref.h``
   * - ``EXECUTOR_OPS_ABI_VERSION``
     - 5
     - ``include/hgraph/runtime/executor_type_ref.h``
   * - ``CLOCK_OPS_ABI_VERSION``
     - 1
     - ``include/hgraph/runtime/clock_type_ref.h``

.. _ops-catalogue-access-modes:

Access modes
------------

The two tag bits on every typed pointer encode
``ReadOnly`` / ``Writable`` / ``Mutation``. The transitions are checked:

- ``begin_mutation`` requires a Writable or Mutation pointer **and**
  the record capability ``Mutable``; on ``ValueView`` it additionally
  requires the ops table to opt in (``allows_mutation``).
- ``mutable_data`` / ``mutable_payload`` / ``try_mutable_as`` require
  ``Mutation`` — a merely Writable pointer refuses them. Constructing a
  view over raw memory yields Writable, so **typed in-place fast paths
  must perform the explicit** ``begin_mutation()`` **re-tag or they
  silently never engage** (this was the PR #479 review finding; the
  engagement probe in ``tests/cpp/test_audit_behavior.cpp`` pins it).

Value-layer ops
---------------

``ValueOps`` (``include/hgraph/types/value/value_ops.h``) is a passive
struct of function pointers. The first member is the ``ValueOpsKind``
discriminant; ``context`` comes next and is passed as the **first
argument** to every slot. Groups of slots:

.. list-table::
   :header-rows: 1
   :widths: 22 50 28

   * - Group
     - Slots
     - Default when null
   * - compare / hash
     - ``hash`` · ``equals`` · ``compare``
     - ``hash`` **throws**; ``equals`` degrades to pointer identity;
       ``compare`` degrades to null-ordering
   * - render
     - ``to_string`` · ``format_string``
     - empty string; ``format_string`` falls back to ``to_string``
   * - python bridge
     - ``to_python`` · ``from_python`` · ``to_python_buffer``
     - **throw**
   * - copy / assign
     - ``copy_construct_view`` · ``copy_assign_view`` ·
       ``copy_assign_from`` · ``move_assign_from``
     - fall back to the **plan's** ``LifecycleOps``
   * - source gating
     - ``accepts_source`` · ``can_materialize_source``
     - ``accepts_source`` defaults to same-plan;
       ``can_materialize_source`` defaults to **false** — an incomplete
       indexed source is rejected, never materialised
   * - projection
     - ``owning_type`` · ``concrete_type`` · ``concrete_memory`` ·
       ``mutable_concrete_memory`` · ``writable_concrete_memory``
     - identity
   * - metrics
     - ``dynamic_storage_metrics``
     - empty metrics

.. note::

   The null-slot policy is deliberately per-slot (a missing ``hash`` is
   an error, a missing ``equals`` is a semantic default). When adding a
   slot, record its default in the table above.

.. admonition:: Recorded exceptions (audit 2026-08-16, accepted)

   - ``CyclicBufferValueOps::head`` and ``QueueValueOps::front`` take
     only ``(memory)`` — no leading context. Aligning them costs a
     ValueOps ABI bump for zero functional gain; accepted as-is. Do not
     copy the shape into new slots.
   - ``equals_impl`` may throw while ``compare_impl`` is ``noexcept``;
     ``ValueView::compare`` fences its semantic fallback with
     ``fallback_on_exception``. The asymmetry is cosmetic and the fence
     is load-bearing — changing either signature is an ABI bump with no
     behavioural benefit.
   - ``TSDataOps.copy_value_from`` / ``move_value_from`` return
     *first-for-time*, while ``from_python``'s bool is cross-checked
     against modification recording — same shape, different meaning.
     Read the member docs before implementing a new strategy.
   - The map key-set adapter records intern the **map's** plan under a
     Set schema (the projection reuses the map's storage); this is the
     one sanctioned violation of "the plan describes the schema's
     layout".
   - Per-kind delta policy is declared in ``ts_data/ops.h``, installed
     by the metadata factories, and implemented in ``ts_delta.cpp`` —
     three homes by design: the free functions ARE the kind policy and
     the factories select them once. Consolidating would re-architect
     the factory split for locality alone; navigate via this catalogue.
   - Compact container storages carry their element binding both in the
     plan's state and per instance (8+ bytes each). Removing the copy
     means per-instantiation ops contexts instead of shared statics —
     deferred until a measured need; do not add new per-instance
     duplicates.

Container ops derive from ``ValueOps`` by struct inheritance; the
``ValueOpsKind`` discriminant makes narrowing safe.
``try_value_ops<Ops>`` / ``checked_value_ops<Ops>`` validate the kind
against the compatibility lattice before the downcast:

.. mermaid::

   classDiagram
      ValueOps <|-- IndexedValueOps
      IndexedValueOps <|-- ListValueOps
      IndexedValueOps <|-- CyclicBufferValueOps
      IndexedValueOps <|-- QueueValueOps
      IndexedValueOps <|-- SetValueOps
      IndexedValueOps <|-- MapValueOps
      ListValueOps <|-- MutableListValueOps
      SetValueOps <|-- MutableSetValueOps
      MapValueOps <|-- MutableMapValueOps
      class ValueOps {
        kind · context · allows_mutation
        hash / equals / compare
        to_string · python hooks
        copy/assign · projection
      }
      class IndexedValueOps {
        size · element_at · element_binding
        make_range · resize · element_valid
      }
      class MutableListValueOps {
        push_back · set_element · erase
        pop_back · clear · push_back_unset
      }
      class SetValueOps { contains }
      class MutableSetValueOps { add · remove · clear }
      class MapValueOps {
        contains · value_at · value_binding
        keys/values/kv ranges · key_set
      }
      class MutableMapValueOps {
        insert · erase · clear · value_or_emplace
      }

.. note::

   ``ValueOpsKind`` is an **ops-shape** tag, not a value-kind tag:
   tuples, bundles, fixed lists, ``Owned[T]`` and polymorphic unions all
   report ``Indexed``. Consult ``schema()->value_kind()`` for the
   semantic kind.

Value specialisation matrix
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``ValuePlanFactory::synthesise_type``
(``src/hgraph/types/metadata/value_plan_factory.cpp``) is the single
switch mapping schema kind × flags (owned, mutable, fixed-size, shaped
array, storage variant) to a concrete plan + ops pair:

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Value kind
     - Storage
     - Ops installed
     - Installer
   * - atomic scalar
     - raw ``T``
     - ``ops_for<T>()`` (kind Base)
     - ``TypeRegistry::register_scalar<T>``
   * - enum
     - ``Int``
     - ``enum_ops_for(meta)`` (member-aware)
     - ``type_registry.cpp``
   * - tuple / bundle (fixed)
     - flat composite bytes + hidden validity words
     - ``composite_indexed_ops`` (per schema)
     - ``value_plan_factory.cpp``
   * - list, fixed
     - array plan (stride × count)
     - ``array_indexed_ops``
     - ``value_plan_factory.cpp``
   * - list / set / map / cyclic-buffer / queue, compact
     - ``ListStorage`` … ``QueueStorage``
       (``compact_storage.h``)
     - ``compact_*_ops`` statics
     - ``compact_container_ops.h``
   * - list / set / map, mutable
     - ``Mutable*Storage`` (slot stores)
     - ``mutable_*_ops`` statics
     - ``mutable_container_ops.h``
   * - map key-set adapter
     - *reuses the map's storage and plan*
     - ``*_map_key_set_ops`` (read-only Set surface)
     - ``compact_container_ops.cpp``
   * - ``Any``
     - embedded owning ``Value``
     - ``any_ops()`` (kind Base)
     - ``any_ops.cpp``
   * - ``Owned[T]`` indirection
     - one-word ``OwnedAllocation*``
     - per-entry ``IndexedValueOps``
     - ``value_plan_factory.cpp``
   * - polymorphic union (inline / pooled)
     - tag + max payload, or one-word pooled handle
     - per-entry ``IndexedValueOps``
     - ``type_realization.cpp`` /
       ``pooled_polymorphic_value_type.cpp``
   * - python-retained / python-owned bundle
     - ``PythonValueHolder`` / ``PythonBundleValue``
     - per-entry ops (GIL-taking)
     - ``value_plan_factory.cpp`` / ``bridge_state.cpp``

TS-layer ops
------------

``TSDataOps`` (``include/hgraph/types/time_series/ts_data/ops.h``) is
the root table for every time-series payload+delta structure. Its
context is the interned per-shape layout/context object; generic policy
(validity checks, parent stamping, modification recording) lives on
``TSDataView`` / ``TSDataMutationView``, never in the table.

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - Group
     - Members
   * - identity / policy
     - ``kind`` · ``context`` · ``allows_mutation`` ·
       ``indexed_child_growth`` · ``direct_native_value`` ·
       ``is_target_link``
   * - sub-tables
     - ``ownership_ops`` (nullable) · ``inspection_ops`` ·
       ``current_state_ops`` · ``python_ops``
   * - layout / tracking
     - ``layout`` · ``tracking`` · ``mutable_tracking``
   * - validity
     - ``has_current_value`` · ``all_valid``
   * - value / delta read
     - ``value_view`` (nullable override) · ``delta_view`` (nullable) ·
       ``value_memory`` · ``mutable_value_memory`` · ``delta_memory`` ·
       ``mutable_delta_memory``
   * - mutation
     - ``copy_value_from`` · ``move_value_from`` (bool =
       *first-for-time*, not success)
   * - delta capture / apply
     - ``empty_delta`` · ``capture_delta`` · ``delta_has_effect`` ·
       ``apply_delta`` · ``clear_collection`` ·
       ``record_child_modified``
   * - children
     - ``indexed_child_count`` · ``indexed_child_binding`` ·
       ``indexed_child_memory`` · ``mutable_indexed_child_memory``
   * - python (opt-in build)
     - ``from_python`` · ``to_python`` · ``delta_to_python``

``direct_native_value`` is the **typed fast-path gate**: set only by
pure-native atomic storages (never for python-cached or python-only
variants). On the write side it lets
``TSDataMutationView::mutable_value()`` hand out a Mutation-tagged
``ValueView`` over the raw payload so callers such as
``Out<TS<T>>::set`` can assign in place and commit with
``mark_modified()``, skipping the erased copy pipeline. On the read
side ``TSDataView::try_native_value_memory`` /
``TSInputView::try_native_value_memory`` return the payload address
under the same gate plus an ops-address identity check against
``ops_for<T>``, so ``In<TS<T>>::value()`` and the lifted kernels load
the native value directly and fall back to the erased
``value().checked_as<T>()`` path otherwise.

Derived tables and sub-tables:

.. mermaid::

   classDiagram
      TSDataOps <|-- TSSDataOps
      TSSDataOps <|-- TSDDataOps
      TSDataOps <|-- IndexedTSDataOps
      TSDataOps <|-- TSWDataOps
      TSDataOps ..> TSDataOwnershipOps : ownership_ops (nullable)
      TSDataOps ..> TSDataInspectionOps : inspection_ops
      TSDataOps ..> TSCurrentStateOps : current_state_ops (per kind)
      TSDataOps ..> PythonTSDataOps : python_ops (per kind)
      class TSSDataOps {
        slot surface: size · occupied · added/removed
        insert/remove key · touch · ranges
        slot observers
      }
      class TSDDataOps {
        child_at_slot · slot_modified
        structural delta · 12 range makers
      }
      class IndexedTSDataOps {
        size · element access
        dense modified-index (nullable)
      }
      class TSWDataOps {
        window: push · element/time_at
        capacity · evicted/cleared
      }

The four sub-tables each answer one concern:

- ``TSCurrentStateOps`` — record/replay reconciliation: eight per-kind
  singleton tables selected once at factory time
  (``src/hgraph/types/time_series/ts_delta.cpp``).
- ``python_bridge::PythonTSDataOps`` — delta conversion and node-result
  application per kind, plus a target-link alias table that
  canonicalises through the bound target.
- ``detail::TSDataOwnershipOps`` — owned-child enumeration used by the
  parent-attachment / stop / invalidate tree walkers.
- ``TSDataInspectionOps`` — cold-path field metadata consumed when
  interning debug descriptors; only fixed bundles install a real table.

TS specialisation matrix
~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 16 22 20 42

   * - TS kind
     - Ops struct
     - Context object
     - Notes
   * - ``TS`` / ``SIGNAL`` / ``REF``
     - ``TSDataOps``
     - ``AtomicTSDataOpsEntry``
       (``ts_data_atomic_ops.cpp``)
     - delta memory aliases value memory; storage-variant selection
       (Native / NativeWithPythonCache / PythonOnly) happens here;
       ``direct_native_value`` true only for Native
   * - ``TSB`` / fixed ``TSL``
     - ``IndexedTSDataOps``
     - ``FixedTSDataContext``
       (``ts_data_fixed_structured_ops.cpp``)
     - children at planned offsets; recursion interns per-child records
   * - dynamic ``TSL``
     - ``IndexedTSDataOps``
     - ``DynamicTSLContext``
       (``ts_data_dynamic_list_ops.cpp``)
     - grow-only child slots; sole user of ``indexed_child_growth``
   * - ``TSS``
     - ``TSSDataOps``
     - ``TSSContext``
       (``ts_data_slot_ops.cpp``)
     - slot store + added/removed bitsets; lazy delta-window roll
   * - ``TSD``
     - ``TSDDataOps``
     - ``TSDContext``
       (``ts_data_slot_ops.cpp``)
     - inherits the TSS key surface; owns child TSData slots; exposes a
       read-only key-set projection with dedicated tracking
   * - ``TSW`` (tick / duration)
     - ``TSWDataOps``
     - ``SizeTSWContext`` / ``TimeTSWContext``
       (``ts_data_window_ops.cpp``)
     - ring storage; validity policy differs per strategy
   * - target-link alias (input side, every kind)
     - matching derived struct
     - ``TSInputTargetLinkContext``
       (``ts_input/target_link_ops.cpp``)
     - no owned payload — forwards to the bound target; the only tables
       installing ``value_view`` / ``delta_view`` overrides
   * - ``TSD`` proxy (REF adaptation)
     - ``TSDDataOps`` (read-only)
     - ``TSDProxyContext`` (``ts_data/proxy.cpp``)
     - mirrors a source TSD through reference adaptation

Every context object is interned in a per-family, mutex-guarded map
(counted ``TypeSystemMutex`` — build-time only, per the 2026-07-02
lock-free ruling). Contexts also intern the **value-layer** ops whose
records become ``layout.value_binding`` / ``delta_binding`` — the TS
layer manufactures ``ValueTypeRef`` s that project live TSData as plain
value views.

Dispatch chains
---------------

The typed write path (``Out<TS<T>>::set``):

.. mermaid::

   flowchart TD
      SET["Out&lt;TS&lt;T&gt;&gt;::set(value)"] --> BM["TSOutputView::begin_mutation(et)<br/>re-tags pointer Writable → Mutation"]
      BM --> MV["TSDataMutationView::mutable_value()"]
      MV --> GATE{"ops.direct_native_value<br/>and value ops allow mutation?"}
      GATE -- yes --> TRY{"try_mutable_as&lt;T&gt;()"}
      TRY -- match --> FAST["assign in place<br/>mark_modified()"]
      GATE -- no --> ERASED["erased path:<br/>copy_value_from → value-layer copy_assign_from"]
      TRY -- mismatch --> ERASED
      FAST --> REC["record_modified_local<br/>observers.notify"]
      ERASED --> FFT{"first for time?"}
      FFT -- yes --> REC
      REC --> PARENT["notify_parent_modified<br/>TSParentLink bubble to endpoint"]

The read path (``In<TS<Int>>::value()``, active peered slot):

.. mermaid::

   flowchart TD
      VAL["In&lt;TS&lt;Int&gt;&gt;::value()"] --> CUR["InputDataCursor::resolved_value_data()"]
      CUR --> TRUST{"route bound · locally active ·<br/>value observation · target bound?"}
      TRUST -- yes --> DV["observed.data_view()<br/>(handle resolved at subscribe time)"]
      TRUST -- no --> SLOW["target_link_resolve<br/>full path projection"]
      DV --> GATE{"ops.direct_native_value and<br/>value ops == ops_for&lt;Int&gt;?"}
      SLOW --> GATE
      GATE -- yes --> FASTR["try_native_value_memory:<br/>load the Int directly"]
      GATE -- no --> TSV["TSDataView::value()"]
      TSV --> OPS["ops.layout / ops.value_memory<br/>(fn-ptr hops)"]
      OPS --> VV["ValueView · concrete()"]
      VV --> CA["checked_as&lt;Int&gt;<br/>(atomicity + ops-address identity checks)"]
      CA --> LOAD["load the Int"]

Terminology notes
-----------------

- The parent-link type is ``TSParentLink`` (kinds in
  ``TSParentLinkKind``; terminal interface ``TSDataParent``). Older doc
  revisions used the name *TSDataParentLink*, which never existed in
  code.
- ``TSDataTypeRef`` / ``TSInputTypeRef`` / ``TSOutputTypeRef`` are
  aliases of ``BasicTSTypeRef<TypeRole>``, not distinct classes.
- The word *binding* survives only in the runtime-connection sense
  (``TSOutputView::binding_for`` negotiates output representation for a
  binding input). As **type metadata** the term is retired — the anchor
  is the ``TypeRecord`` and its TypeRefs
  (:doc:`../unified_type_erasure`).
- ``TSState`` remains conceptual vocabulary only; no such C++ type
  exists.
