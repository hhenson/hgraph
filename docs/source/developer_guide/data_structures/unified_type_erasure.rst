Unified Type Erasure
====================

Status
------

This chapter describes the implemented common type-erasure model. The
historical problems and migration sequence are retained to explain the design
constraints; :doc:`type_erasure_inventory` records the pre-migration baseline.

The runtime needs type erasure because graphs contain values, time-series,
nodes, and nested graphs whose concrete C++ types are not all known to the
execution loop. Type erasure itself was not the problem. Before this work each
erased family had its own descriptor shape, and the family was usually known
only from the surrounding C++ template type. An arbitrary descriptor pointer
therefore had no reliable common header for debugger navigation.

The implemented model has two goals:

* every erased runtime object points at one common, self-describing type record;
* the pointer used on hot paths remains two machine words and can acquire a
  family-specific C++ API without becoming larger.

This is similar to a Python object only at a structural level: a type pointer
and a data pointer travel together.  It is not intended to introduce Python
object semantics, reference counting, heap allocation, or a single universal
virtual interface into the C++ runtime.

Problems In The Baseline Model
------------------------------

The Milestone 0 implementation already contained useful parts of this design:

* ``TypeBinding<Schema, Ops>`` is consistently a schema pointer, storage plan
  pointer, and ops pointer;
* ``StorageRef<Binding>`` is a two-word borrowed cursor containing a binding
  pointer and a data pointer;
* schemas, storage plans, bindings, and builders are interned or cached by
  their respective registries and factories;
* value and time-series views carry their erased type information without
  requiring C++ RTTI.

However, that common shape was a template convention rather than a runtime
contract. In particular:

* ``TypeBinding<ValueTypeMetaData, ValueOps>`` and
  ``TypeBinding<NodeSchema, NodeOps>`` have compatible-looking layouts, but an
  arbitrary binding pointer cannot identify which schema and ops ABI it uses;
* value and time-series schemas share some metadata, while node, graph,
  executor, and clock schemas use unrelated structures;
* the word *binding* is also used for time-series input/output linking, making
  it unclear whether a type identity or a runtime connection is meant;
* ownership, borrowed access, mutability, and type identity are combined in
  different ways by ``StorageHandle``, ``StorageRef``, and the various views;
* a storage plan describes layout and lifetime, but generally cannot tell a
  debugger the semantic type of a child at a given offset;
* GDB and LLDB printers must recognize concrete template names and private
  field layouts.  At an unknown address they must guess the family before they
  can interpret the record.

The result is difficult manual navigation and fragile pretty printers.  Adding
more family-specific printers cannot solve the missing runtime discriminator.

Vocabulary
----------

The following terms should be used consistently throughout the runtime and its
documentation:

Schema
   The interned semantic identity of a type: what the object means.  Examples
   include ``int64``, ``TSD[str, TS[int64]]``, an ``add`` node, or a graph with
   a particular interface.  A schema does not select a storage representation.

Plan
   The immutable physical layout and lifetime recipe: size, alignment, child
   layout, construction, destruction, and allocation requirements.

Ops
   A narrow, family- and role-specific behaviour table.  Value ops, node ops,
   and graph ops remain different ABIs.

Type record
   The interned, resolved combination of schema, plan, ops, role, and debug
   metadata.  This replaces *type binding* as the preferred term.  A type
   record describes one concrete runtime representation of a schema.

Pointer
   A non-owning pair of type-record pointer and data pointer.  It may be generic
   or exposed through a family-typed wrapper.  A pointer never destroys data.

Owner
   An object which owns storage and its lifetime, possibly inline and possibly
   through an allocator.  It can produce a pointer to its storage.

View
   A family-specific API facade over a pointer.  A view neither supplies type
   identity nor owns the data independently of its pointer.

Builder
   A reusable construction recipe which has resolved the required schema,
   plan, ops, and type record.

Registry or factory
   Infrastructure which validates and interns schemas, plans, type records,
   and builders.  Family factories remain typed front ends; they need not be
   replaced by one monolithic factory.

The unqualified words *descriptor*, *handle*, *reference*, and *value* should
not be used for these structures.  They are too overloaded to convey ownership
or layer.  *Definition* is suitable for mutable input to a factory; *type
record* is the canonical resolved result.  *Binding* remains appropriate for
connecting a time-series input to an output, but should be retired as the name
of type metadata.

The intended relationship to current structures is:

.. list-table:: Historical-to-current mapping
   :header-rows: 1
   :widths: 30 30 40

   * - Historical concept
     - Current concept
     - Structural effect
   * - Family metadata such as ``ValueTypeMetaData`` and ``NodeSchema``
     - ``SchemaHeader`` plus a typed schema body
     - Adds a common prefix or common referenced header; retains family data.
   * - ``TypeBinding<Schema, Ops>``
     - ``TypeRecord``
     - Replaces template-only identity with one runtime-readable layout.
   * - ``StorageRef<Binding>``
     - ``AnyPtr`` or ``TypedPtr``
     - Retains the two-word borrowed representation and adds validation.
   * - Borrowed state in ``StorageHandle``
     - Pointer
     - Moves non-owning access out of the owning abstraction.
   * - Inline or allocated state in ``StorageHandle``
     - ``ErasedOwner``
     - Retains storage optimisation with unambiguous lifetime authority.
   * - ``ValueView``, time-series views, and runtime wrappers
     - Family view over a typed pointer
     - Retains rich APIs without duplicating identity or ownership.
   * - ``StoragePlan`` and family ops structures
     - Plan and Ops
     - No conceptual merger; both remain specialised and immutable.
   * - Family registries and factories
     - Typed factories plus ``TypeRecordRegistry``
     - Adds common validation and interning after family-specific resolution.

Classification
--------------

One discriminator is not sufficient.  The system needs three orthogonal
classifications, stored at the layer which owns their meaning:

Family
   Selects the schema family.  Initial families are ``Value``, ``TimeSeries``,
   ``Node``, ``Graph``, ``Executor``, and ``Clock``.  Family is part of
   semantic identity and therefore belongs to the schema.

Role
   Selects a representation within a family.  Time-series ``Data``, ``Input``,
   and ``Output`` are roles of one semantic time-series schema.  ``Instance``
   is the usual value role.  Other families can define roles only when they
   have genuinely distinct representations.  Role belongs to the type record
   because it participates in selecting the plan and ops ABI.

Kind
   A family-defined sub-family.  Value kinds include atomic, tuple, list, and
   mapping.  Time-series kinds include TS, TSS, TSL, TSB, TSD, and REF.  Node
   kinds may include compute, sink, generator, and service nodes.  Kind is
   semantic and therefore belongs to the schema.

The first implementation should not store a combined type tag.  Doing so would
duplicate the schema's family and kind in every type record and create an
invariant which all factories must maintain.  Code which needs a single value
can construct a compact classification from the schema and record:

.. code-block:: cpp

   enum class TypeFamily : std::uint8_t {
       Invalid,
       Value,
       TimeSeries,
       Node,
       Graph,
       Executor,
       Clock,
   };

   enum class TypeRole : std::uint8_t {
       Invalid = 0,
       Instance = 1,
       Data = 2,
       Runtime = 3,
       Input = 4,
       Output = 5,
   };

   struct TypeClassification {
       TypeFamily family;
       TypeRole role;
       std::uint8_t kind;
   };

``kind`` is intentionally numeric in the common layer.  Its enum and meaning
belong to the selected family.  This avoids forcing every specialization into
one ever-growing global enum while leaving enough information for a debugger
to dispatch correctly.  If profiling later demonstrates that following the
schema pointer to read family or kind affects an important hot path, a packed
classification may be cached on ``TypeRecord`` as a deliberate optimisation.
That cache must be populated by the central registry and checked against the
schema during construction and in debug builds.

Common Runtime Records
----------------------

Every semantic schema should begin with, or provide a stable pointer to, a
common header.  Every resolved type record should have one non-template layout.
The following illustrates the information and relationships rather than a
final byte layout:

.. code-block:: cpp

   struct SchemaHeader {
       std::uint32_t magic;
       std::uint16_t abi_version;
       TypeFamily family;
       std::uint8_t kind;
       const char *label;
       const SchemaIntrospection *introspection;
   };

   struct TypeRecord {
       std::uint32_t magic;
       std::uint16_t abi_version;
       TypeRole role;
       std::uint8_t ops_abi_version;
       TypeCapabilities capabilities;
       const char *implementation_label;
       const SchemaHeader *schema;
       const StoragePlan *plan;
       const void *ops;
       const DebugDescriptor *debug;
   };

The labels answer different questions and live on the records which own their
meaning:

* ``schema->label`` is the stable semantic name a user expects, such as
  ``TSD[str, TS[int64]]``.  It is stored only on the schema;
* ``implementation_label`` identifies the selected representation, such as a
  compact tuple, slot-store dictionary, Python node, or native compute node.
  It is a contribution of the type record.

The implementation label can be null when the semantic label is sufficient.
The schema registry owns semantic labels and the type-record registry owns
implementation labels.  A magic value and ABI version on each independently
addressable common record allow a debugger to reject unrelated or incompatible
memory instead of guessing.  The schema ABI versions the schema-header layout;
the type-record ABI versions the record layout; the ops ABI versions the table
selected by family and role.

The value-family pilot makes this concrete: ``ValueTypeMetaData`` is a
standard-layout type whose first member is ``SchemaHeader`` and whose second
member is ``ValueTypeFlags``.  It does not inherit the legacy family metadata.
Its kind values are the fixed numeric range ``Atomic=0`` through ``Any=8``;
conversion from the compact header kind is checked before family-specific
dispatch.  ``TypeRegistry`` owns a non-empty canonical label for every value
schema before interning it.  Composite labels use forms such as
``Tuple[A,B]``, ``Bundle{field:A}``, ``List[A,4]``, ``Map[K,V]``, and
``Queue[A]``; unresolved children are rendered as ``<unresolved>``.  A named
bundle uses its nominal name, while its structural twin retains the
``Bundle{...}`` form.  Registry aliases are lookup names only and never mutate
that canonical label or affect schema identity.

Value ops tables carry a separate one-byte ``ValueOpsKind`` discriminator at
offset zero.  It describes the concrete ops-table ABI, not the semantic value
schema.  The supported hierarchy is ``Base <- Indexed`` with the leaf branches
``List <- MutableList``, ``CyclicBuffer``, ``Queue``, ``Set <- MutableSet``, and
``Map <- MutableMap``.  ``try_value_ops`` and ``checked_value_ops`` are the only
production base-to-derived narrowing boundary.  They validate this hierarchy
before any derived hook is inspected; null, invalid, and unknown tags are
rejected.  This distinction is intentional: a fixed list schema may publish an
``Indexed`` table and supports indexed access, but it must not be treated as a
``ListValueOps`` object.  Schema kind therefore never implies ops layout.

The value ops ABI also has an optional cold-path ``dynamic_storage_metrics``
hook. It reports only heap storage exclusively owned by the referenced payload;
fixed bytes described by the storage plan are not repeated. Built-in compound
and container implementations recurse through this hook, while extension ops
may leave it null and report zero. Generic diagnostics can therefore aggregate
storage without inspecting a concrete representation or running allocation
estimates on the evaluation path.

``TypeRecord`` does not own its schema, plan, ops, or debug metadata.  All are
immutable and have stable addresses for at least the lifetime of the registry.
Production registries will normally keep them for the process lifetime.  This
makes type-record pointer equality a valid identity comparison.

ABI Evolution Policy
--------------------

Version one freezes the field order, field widths, alignment, and meaning of
``SchemaHeader``, ``TypeRecord``, ``AnyPtr``, and the version-one debug
descriptor structures.  It does not freeze C++ constructors, registry or owner
classes, storage-plan implementations, ops-table layouts across different
``ops_abi_version`` values, or the address chosen for any interned object.

The following rules apply when these records evolve:

* adding, removing, reordering, resizing, or changing the meaning of a frozen
  field requires incrementing the ABI version of that independently readable
  structure;
* a family ops-table change increments that family's ops ABI version and every
  consumer must validate it before casting ``TypeRecord::ops``;
* reserved fields remain zero in the current ABI.  Giving a reserved bit or
  byte a meaning is an ABI change unless version-one readers are explicitly
  specified to ignore it;
* producers populate magic and version fields through the central registry or
  descriptor factory.  Consumers reject unknown versions before following
  schema, plan, ops, debug, or data pointers;
* appending a separately versioned structure and pointing to it may preserve
  the parent ABI when a null pointer retains its old meaning.  Embedding the
  same fields into a frozen parent does not;
* changing only an implementation label or immutable metadata contents does
  not change layout ABI, but changing semantic identity requires a distinct
  interned schema or type record.

Every accepted ABI change must update the compile-time size/offset assertions,
the separately compiled shared-library boundary fixture, debugger decoder
version checks, and installed-package consumer.  ABI version constants are
monotonic; an old value is never repurposed after release.

The common record must not grow a universal ops table.
``schema->family`` and ``role`` select the expected narrow ops ABI, and ``ops``
points at that ABI.  A ``NodePtr`` can therefore call node ops directly while
an ``AnyPtr`` can report that node operations exist without pretending to
expose them all.

Capabilities describe operations which are valid for generic infrastructure,
not replacements for the family ops table.  Candidate bits include
``Constructible``, ``Destructible``, ``Copyable``, ``Movable``, ``Mutable``,
``Comparable``, ``Hashable``, ``HasChildren``, and ``Viewable``.  Type-record
construction must validate that mandatory capabilities and ops are consistent
with the family and role.

The Pointer Model
-----------------

The universal borrowed representation should remain exactly two machine
words:

.. code-block:: cpp

   enum class AccessMode : std::uintptr_t {
       ReadOnly = 0,
       Writable = 1,
       Mutation = 2,
   };

   using TaggedTypeRecordPtr = tagged_ptr<const TypeRecord, 2, AccessMode>;

   class AnyPtr {
       TaggedTypeRecordPtr type_;
       const void *data_;
   };

   template<TypeFamily Family, TypeRole Role = TypeRole::Invalid>
   class TypedPtr {
       AnyPtr value_;
   };

   using NodePtr = TypedPtr<TypeFamily::Node, TypeRole::Runtime>;
   using GraphPtr = TypedPtr<TypeFamily::Graph, TypeRole::Runtime>;
   using TSDataPtr = TypedPtr<TypeFamily::TimeSeries, TypeRole::Data>;

``AnyPtr`` provides safe generic inspection: family, role, kind, labels,
capabilities, plan, and raw data address.  Conversion from ``AnyPtr`` to a
``TypedPtr`` validates ``type_->schema->family`` and ``type_->role`` once.  Code
which already knows the family uses the typed wrapper, whose C++ type encodes
that knowledge without adding another runtime word.  Family views then wrap
the typed pointer and expose their normal APIs.

For example, the graph compiler can store and pass ``NodePtr``.  A debugger can
still interpret its first word as a ``TypeRecord`` and discover the exact node
schema.  Generic graph diagnostics can accept ``AnyPtr``.  Converting the same
object to a time-series pointer fails immediately rather than reinterpreting
its ops table.

The low two alignment bits of the type-record pointer encode access state.
``ReadOnly`` can inspect live data, ``Writable`` can begin mutation only when
the record has the ``Mutable`` capability, and ``Mutation`` alone exposes a
mutable data address.  Access can be downgraded to ``ReadOnly`` but never
escalated from it.  Beginning mutation is idempotent; ending it returns to
``Writable``.  These access modes are pointer properties, not type identity,
and are masked before inspecting the record.
``read_only_access()``, ``writable_access()``, and ``mutation_access()`` query
the encoded access state without implying an access transition.

``TypedPtr<Family, TypeRole::Invalid>`` is a family wildcard.  A specific
typed pointer widens implicitly to ``AnyPtr`` or its same-family wildcard
without revalidation.  Narrowing from ``AnyPtr`` validates the exact family
and, for a specific typed pointer, the exact role.  Narrowing from a family
wildcard to a specific role is therefore explicit and checked.  Cross-family
conversion goes through ``AnyPtr``.

The valid null states should be explicit:

* ``{nullptr, nullptr}`` is unbound;
* ``{type, nullptr}`` with ``ReadOnly`` access is a typed null;
* ``{nullptr, data}`` is invalid;
* ``{type, data}`` with a known access tag is a live borrowed pointer;
* writable or mutation typed-null states and tag value 3 are invalid.

Pointer equality compares the masked type-record pointer and data address, so
access mode does not affect object identity.  ``same_access_as`` compares only
the access mode, while ``same_state_as`` compares both encoded words including
the access tag.  Typed nulls therefore compare equal only when they carry the
same record, and unbound pointers compare equal.

Ownership is separate. ``ErasedOwner`` retains the inline/heap allocation
optimisation and allocator reference, but has no borrowed mode or reference
factory. Its only live states are owning-inline and owning-heap. Externally
owned or in-place graph storage produces an ``AnyPtr`` or typed pointer
directly. Destruction is performed only by the owner or by the graph/slot
lifetime protocol, never by a borrowed pointer.

``Value`` uses ``ErasedOwner`` exclusively. Destructive time-series assignment
accepts an rvalue writable ``ValueView`` as its erased source; the ``Value&&``
surface is a convenience which supplies that view. This permits moving a child
from a larger externally owned value without fabricating a non-owning
``Value``. Read-only views are rejected before dispatch.

``GraphValue`` stores a ``GraphPtr`` followed by an optional ``ErasedOwner``.
Ordinary root/nested graphs point into their owner. Slot-placed nested graphs
have no owner and point into graph/slot memory, whose stop/delete and
destructor/erase protocol remains the lifetime authority. The uniform pointer
costs one additional word compared with the former owner-plus-boolean layout:
five words instead of four on 64-bit builds; ``ErasedOwner`` itself remains
three words and all borrowed pointers remain two words.

This makes the following invariants visible in the types:

* pointers are cheap to copy and never affect lifetime;
* owners cannot silently become references;
* views cannot outlive the storage merely because they carry a type record;
* in-place graph state uses the same pointer representation as heap-owned state.

Debug And Introspection Protocol
--------------------------------

A pretty printer attached to a stopped process should not need to call runtime
methods.  Function calls can be unavailable for core files, unsafe while
threads are stopped, and difficult to reproduce in both GDB and LLDB.  The
minimum inspection protocol must therefore be data driven.

The common record guarantees shallow inspection of every erased pointer:

* validity, access state, and data address;
* the schema's semantic label and the record's implementation label;
* family, role, kind, and the relevant schema, record, and ops ABI versions;
* size and alignment from the plan;
* schema, plan, ops, and debug-descriptor addresses;
* capabilities and whether deeper navigation is available.

Deeper inspection is described by an immutable ``DebugDescriptor`` produced by
the same factory that resolves the type record. The data-only layouts are:

.. code-block:: cpp

   enum class DebugLayoutKind : std::uint8_t {
       Opaque,
       Atomic,
       FixedComposite,
       Sequence,
       KeyedSlots,
       Node,
       Graph,
   };

   enum class DebugAtomicKind : std::uint8_t {
       Opaque,
       Boolean,
       SignedInteger,
       UnsignedInteger,
       FloatingPoint,
   };

   struct DebugField {
       const char *name;
       std::size_t offset;
       const TypeRecord *type;
       std::uint32_t validity_bit;
       DebugFieldFlags flags;
   };

   struct DebugDescriptor {
       std::uint32_t magic;
       std::uint16_t abi_version;
       DebugLayoutKind layout;
       DebugAtomicKind atomic_kind;
       DebugDescriptorFlags flags;
       std::uint32_t field_count;
       const DebugField *fields;
       std::size_t validity_offset;
       std::uint32_t validity_word_size;
       std::uint32_t reserved0;
       const TypeRecord *key_type;
       const TypeRecord *element_type;
       const DebugDynamicLayout *dynamic_layout;
   };

   struct DebugDynamicLayout {
       std::uint32_t magic;
       std::uint16_t abi_version;
       DebugDynamicKind kind;
       std::uint8_t reserved0;
       DebugDynamicFlags flags;
       std::uint32_t reserved1;
       std::size_t size_offset;
       std::size_t size_constant;
       std::size_t data_offset;
       std::size_t stride;
       std::size_t key_data_offset;
       std::size_t key_stride;
       std::size_t state_offset;
       std::size_t auxiliary_offset;
       std::size_t entry_offset;
   };

The descriptor is 64 bytes and each fixed field is 32 bytes on supported
64-bit platforms. Atomic representation is selected from the registered C++
type, never inferred from a semantic label: bool, signed/unsigned integers, and
32/64-bit floating point values can be decoded directly; other atomic storage
remains explicitly opaque. Fixed-composite fields connect semantic child type
records to physical plan offsets. Tuple and bundle descriptors also publish the
validity-word offset and one bit index per field, so an unset child is displayed
as typed-null rather than reading uninitialised payload bytes.

The version-three dynamic layout remains 88 bytes on supported 64-bit
platforms. It distinguishes contiguous storage from stable pointer slots and
records whether
size is fixed, data is indirect, a pointer table is used, a ring head is
present, or keys/elements are embedded erased owners or typed pointers. Version
two adds independent tagged-data-pointer, tagged-key-pointer, and tagged-slot-
state flags. Version three also describes an erased stable-slot strategy whose
index lives behind an implementation pointer. In that form ``data_offset``
(and ``key_data_offset`` for maps) locates the implementation pointer,
``auxiliary_offset`` locates the value pointer table within that implementation,
and ``key_auxiliary_offset`` independently locates a keyed store's pointer
table. Debugger readers clear the implementation handle's low two private tag
bits before applying those offsets; this is a no-op for the former raw erased-
owner pointer. The size/state offsets are interpreted in the key implementation
when their corresponding flags are set. For a tagged stable-slot index,
``state_offset`` may identify the same
pointer table as data or keys: readers accept tag ``00`` as live and mask the
low two bits before dereferencing other encoded states. Bitmap-backed stable
slots continue to publish the public ``SlotBitmap`` words and bit count. Both
strategies are exposed through ``StableSlotDebugView``, so descriptor factories
do not inspect the selected implementation or a standard-library container.

The semantic ``StableSlotStore`` facade does not contain a closed
``std::variant`` of those concrete strategies. It is one tagged implementation
pointer whose private tags identify the canonical nop, tagged-pointer, and
bitmap paths. Default and moved-from stores carry the nop tag rather than a
nullable implementation. Operations use an inline switch while the concrete
classes stay under the implementation-file boundary and semantic owners use
only the erased facade. Allocation and destruction continue to use the bound
``AllocatorOps``.

This is the measured closed-strategy refinement of the general ops-table
pattern. Use a passive ops table when independently supplied implementations
must extend a public behavioural contract. When a small internal strategy set
is closed, its discriminator is not semantic API, and indirect calls are a
measured hot-path cost, a private tagged handle plus inline dispatch preserves
the erased surface without exposing ``std::variant`` ownership to callers.

The common embedded-owner field flag describes the three-word
``ErasedOwner<InlineStoragePolicy<>, TypeRecord>`` ABI. The record is read
from the identity word, ownership state from the tagged allocator word, and
the inline or indirect payload from the storage word. State value three is
reserved and rejected. Node state/scalar fields use this owner protocol.
Nested ``GraphValue`` fields and map/mesh entries instead expose their leading
two-word ``GraphPtr`` through embedded-pointer descriptor flags, so owned and
slot-placed graphs have the same debugger path.

Implemented dynamic navigation covers fixed lists, dense compact lists and
sets, cyclic buffers, queues, mutable lists/sets/maps, graph node allocations,
node state/scalars, single/switch child graphs, and map/mesh keyed child graph
slots. Nullable dynamic lists and compact maps with nullable values remain
opaque because their ``vector<bool>`` validity representation is not a stable
debug ABI. Node input/output endpoint owners also remain shallow until their
specialized ownership layout is published explicitly.

Pretty printers first validate magic and ABI, mask pointer tags, then dispatch
on numeric family and layout values.  They must never infer the payload type
from its bytes.  Unknown families or layouts are displayed as labeled opaque
objects.  A shared generated manifest, or names stored in the records, should
prevent GDB and LLDB scripts from maintaining independent enum maps.

The protocol deliberately has two levels:

1. shallow inspection is mandatory and stable for every type record;
2. deep child navigation is optional, but structured and safe when supplied.

Release builds should retain the shallow metadata.  It consists mostly of
interned cold data and is the part required to diagnose production failures.
Large deep descriptors can be made optional per type if their size is proven to
matter.  An in-process ``inspect(AnyPtr)`` utility may reuse the same metadata
for logging and tests, but is not a substitute for the debugger-readable data.

Factories And Interning
-----------------------

Resolution should follow one visible pipeline:

.. code-block:: text

   Definition
       -> SchemaRegistry::intern
       -> family factory resolves Plan + Ops + DebugDescriptor
       -> TypeRecordRegistry::intern
       -> Builder
       -> Owner or in-place storage
       -> Pointer
       -> View

``TypeRecordRegistry`` is the common boundary.  It validates the schema header
and settled family-role pair, nonzero ops ABI, known capabilities, valid plan,
and nonnull ops pointer.  Its canonical identity key is ``(schema, role, plan,
ops, debug descriptor)``.  The ops ABI, capabilities, and implementation label
are immutable metadata attached to that identity: a same-key disagreement is
rejected, and a different implementation label cannot create a second record.

Family factories remain responsible for family-specific policy.  A
time-series factory knows how TSD data uses a slot store; a node factory knows
which ops a compute node requires.  The common registry only enforces the
cross-family record contract and supplies stable identity.  Plan factories and
schema registries remain separate because the same semantic schema may have
more than one valid physical implementation.

Interning should provide:

* stable addresses and pointer-identity equality;
* registry-owned strings and debug arrays;
* thread-safe lookup and construction;
* no dependency on Python for native records;
* explicit test-only reset semantics, since resetting invalidates pointers.

Python-authored nodes use the same ``Node`` family and node ops ABI.  Their
implementation label and ops select the Python bridge, but the common record,
pointer, graph storage, and debugger protocol remain native C++ structures.

Examples
--------

Atomic Value
~~~~~~~~~~~~

An ``int64`` value has a ``Value`` schema with ``Atomic`` kind.  Its instance
type record selects the compact value plan and integer value ops.  ``AnyPtr``
prints ``int64``, ``Value/Instance/Atomic``, its address, and its layout.  A
``ValuePtr`` adds the value API without changing the two-word representation.

Time-Series Dictionary
~~~~~~~~~~~~~~~~~~~~~~

``TSD[str, TS[int64]]`` has one semantic time-series schema. Its data type
record uses ``TimeSeries/Data/TSD`` and selects the slot-store plan and TSD
data ops. Separate canonical records describe its key-set and element
projections, and Input/Output roles select their own topology records without
duplicating the schema. Scalar ``TS``/``SIGNAL``, fixed and dynamic ``TSL``,
``TSB``, ``TSW``, and keyed/reference roots are record-backed. Generic and
specialised borrowed TSData cursors are two words and recover schema, plan, and
ops from the role record. No parallel time-series binding registry remains.

The keyed-slots debug descriptor identifies key records, slot state, and child
type records.  A debugger can distinguish live, stopped/deleted, and erased
slots according to the slot protocol without guessing the container's concrete
template instantiation.

Compute Node
~~~~~~~~~~~~

An ``add`` compute node has a ``Node`` schema, ``Runtime`` role, and ``Compute``
kind.  The compiler and runner use ``NodePtr`` so their source code and debugger
types retain the known family.  The dynamic record identifies the exact node
schema and native or Python implementation.  A node debug descriptor connects
input, output, and state names to their type records and plan offsets.

Nested Graph
~~~~~~~~~~~~

A map, mesh, or switch instance has a graph or nested-graph type record whose
plan describes its in-place static state.  Its debug descriptor identifies the
slot store and nested graph instance record.  The slot lifecycle remains the
source of truth: delete stops and disconnects an instance; erase invokes its
destructor and makes the slot reusable.  Type erasure supplies navigable type
information but does not replace that ownership protocol.

Completed Migration
-------------------

The implementation followed this incremental sequence:

1. Introduce the family, role, kind, capability, magic, and ABI definitions,
   plus the common ``TypeRecord`` and two-word ``AnyPtr``.
2. Adapt the former ``TypeBinding<Schema, Ops>`` instances to common records at
   factory boundaries.  Add static layout and conversion tests before changing
   call sites.
3. Give value, time-series, node, graph, executor, and clock schemas the common
   schema header while preserving their typed schema bodies.
4. Make family factories intern common records, then expose typed pointer
   wrappers and migrate views to wrap those pointers.
5. Replace template-specialized type bindings with typed accessors over
   ``TypeRecord`` and remove the temporary aliases after migration.
6. Split owning storage from borrowed pointers. Migrate borrowed
   ``StorageHandle`` states to typed pointers, leaving inline and allocated
   state in ``ErasedOwner``.
7. Emit debug descriptors from factories and rewrite GDB and LLDB printers to
   start at ``AnyPtr`` and ``TypeRecord``.
8. Retire the old type-binding registry and borrowed generic storage cursor
   after all families use the new model.

Each phase kept the runtime buildable as pure C++ and was reviewed separately.
The final time-series migration removed the last compatibility registry and
reduced generic and specialised ``TSDataStorageRef`` objects from three words
to the common two-word type/data layout.

Performance And Correctness Gates
---------------------------------

The proposal is acceptable only if it preserves the properties which motivated
the current design:

* ``sizeof(AnyPtr) == 2 * sizeof(void*)`` and typed pointers have the same size;
* typed pointer operations add no family branch after construction;
* no C++ RTTI, virtual base object, reference count, or mandatory heap
  allocation is introduced;
* schemas, plans, ops, type records, and debug descriptors are immutable and
  interned off the hot data path;
* family ops remain direct function-pointer tables where concrete code is not
  known statically;
* pointer tagging is guarded by alignment assertions and tested on all
  supported platforms;
* invalid family/role/ops combinations fail during factory resolution, not at
  their first method call;
* owners, graph plans, and slot stores remain the only lifetime authorities;
* native CMake builds do not depend on Python or the Python bridge.

Tests should cover record interning, invalid conversions, null states, pointer
size and alignment, access tags, owner destruction, schema/plan reuse, and
debug-descriptor traversal.  Debugger tests should decode captured memory
without invoking inferior functions so that the same cases apply to live
processes and core files.

Recommended Decisions For Review
--------------------------------

The following choices are recommended as the starting point:

* Treat time-series Data/Input/Output as roles, not independent families.
* Keep one common, non-template ``TypeRecord`` and typed pointer wrappers; do
  not make the records themselves a class hierarchy.
* Keep the universal pointer at two words and separate all ownership into an
  owner type.
* Make semantic ``label`` mandatory and implementation label optional.
* Store family, kind, and semantic label only on the schema; store role and
  implementation label only on the type record.  Do not cache a combined tag
  until a benchmark justifies it.
* Retain mandatory shallow debug metadata in release builds; allow expensive
  deep descriptors to be optional only after measuring their cost.
* Put access/mutation state in pointer tag bits, never in the interned type
  record.
* Version the record and debug layouts from their first introduction.
* Preserve family-specific factories and ops tables behind the common record
  rather than centralising family policy.

The main design decision still requiring validation is the exact family and
role vocabulary.  It should be tested against values, all time-series roles,
native and Python nodes, graph builders, graph instances, executors, clocks,
services, and nested graph slot stores before the numeric ABI is fixed.
