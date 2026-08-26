RFC 0029: Value Pool Ownership and Binding
==========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-25
:Target: C++ value storage, type realization, graph runtime

Summary
-------

Make the value pool an **explicitly owned and explicitly bound** object.

* **Owned.**  A pool belongs either to the root graph — carried on its
  ``GlobalState`` under a reserved key — or to a single node, held in that
  node's local state.  Both are the same object with the same ops table; only
  the owner differs.
* **Bound.**  An operation that allocates reaches its pool through its own ops
  context, or through the builder it was handed.  Never through ambient state.
  The root graph **binds** its pool into the realization's pooled entries when
  it is constructed and unbinds when it is destroyed; the allocating op reads a
  pointer, and asks nothing about which thread is asking.
* **Confined or synchronised by construction.**  A pool fixes its concurrency
  policy when it is created: ``GraphConfined`` takes no lock anywhere;
  ``Synchronised`` takes one on acquire and on batched return, and never on the
  graph's per-tick path.

The thread-local selection channel — ``active_compound_scalar_storage()``,
``CompoundScalarStorageScope``, and the eight scopes ``graph.cpp`` installs
around construction, lifecycle, and evaluation — is removed.

The rule is absolute, and it is the point of the change: **no pool state is
thread-scoped or process-scoped.**  Pool state lives on the running graph —
on the root graph's storage or on its ``GlobalState``, which at run time is
global only to that graph — or on a node, and nowhere else.  Tying the pool to
the root graph is what makes that possible: a running graph is already the
scope every pooled value belongs to, so nothing has to be found by asking which
thread is asking.

This RFC supersedes the storage-selection mechanism of :doc:`RFC 0013
<rfc_0013_pooled_polymorphic_compound_scalars>`.  Everything else RFC 0013
specifies — per-leaf pools, stable slots, the ``LeafSlotHeader``, non-atomic
counts, and the accounting rule — is unchanged and depended upon here.
:doc:`RFC 0028 <rfc_0028_shared_value_representation>` depends on this RFC for
the pool it allocates from.

Motivation
----------

The pool itself is already owned where it should be.  ``RootGraphRuntimeStorage``
holds the ``CompoundScalarStorage``; a nested graph holds a borrowed view of its
root's (``graph.cpp:566-583``).  What is thread-local is not the pool but the
*selection* of it: a two-word view in
``src/hgraph/types/value/impl/stable_leaf_compound_scalar_storage.cpp:417``,
re-installed by ``CompoundScalarStorageScope`` at eight sites in ``graph.cpp``
(``:598``, ``:1207``, ``:1215``, ``:1223``, ``:1902``, ``:1943``, ``:1997``,
``:2032``).

Three problems follow from that, and the third is what blocks current work.

**Selection is invisible at the point of use.**  A value operation's behaviour
depends on a scope installed in a different translation unit, and the
correctness of every allocating op rests on every entry point into graph code
remembering to install one.  The repository has recorded a preference against
exactly this shape elsewhere — the wiring observer stack is "without a process
global or thread-local observer stack" (``graph_wiring.rst:424``), ``mesh_ref``
resolves through a structural link and is "never a thread-local"
(``mesh.rst:185``), the service build stack has "no thread-locals"
(``services.rst:750``), and the Python bridge evaluates with "neither C++
thread-local state nor a global" (``python_bridge.rst:201``).  The pool's
channel is the outlier.

**Availability is a runtime condition rather than a static one.**  Outside an
installed scope the view is the no-op view, whose allocating entries throw
``unavailable_storage()`` (``stable_leaf_compound_scalar_storage.cpp:329``).
Whether a value of a pooled schema can be constructed therefore depends on
where the constructing code happens to run, and the failure is a
``std::logic_error`` at run time rather than a representation the type system
never offered.

**It costs, on the path that can least afford it.**  ``hgraph`` ships as a
shared library and as a Python extension module, so a ``thread_local`` in it is
not the cheap initial-exec form a statically linked binary would get: on Linux
each access to a dynamically loaded module's thread-local goes through
``__tls_get_addr``, a call that cannot be inlined or hoisted.  Every allocation
of a pooled value pays it, to answer a question — which pool — that the graph
already knows statically.

**There can only ever be one pool in effect.**  The channel holds one view per
thread, so a node cannot own a pool that is live at the same time as its
graph's.  RFC 0028's producer-owned pools need precisely that: a pool owned by
one push node, written by its producer threads, whose slots return to it in a
batch at a cycle boundary, while the graph's own pool continues to serve every
other node.  The ambient channel cannot express two pools, and widening it to a
stack would make the invisibility worse rather than better.

Ownership boundary
------------------

The C++ runtime owns the facility.  It is runtime storage machinery: no
authoring surface changes, no Python type changes, and no schema spelling
changes.  ``GlobalState`` gains one reserved key whose value is an opaque
handle; Python sees a handle it cannot usefully interpret and is not expected
to.

Nothing about the pooled *representation* changes.  A pooled holder is still
one pointer to a stable slot whose ``LeafSlotHeader`` names its owning pool
(``stable_leaf_compound_scalar_storage.cpp:32``), and retain, release, and
destroy still route through that header without consulting anything ambient.
Only **allocation** — ``default_value``, ``copy_value``, ``move_value`` — needs
to be told which pool to use, and this RFC is about how it is told.

The pool object
---------------

``CompoundScalarStorage`` is generalised to ``ValuePool``, and
``CompoundScalarStorageView`` to ``ValuePoolView``.  The shape is retained
exactly: an owning facade over a hidden per-leaf registry, and a two-word
borrowed view (context + non-null ops table) that is trivially copyable.  The
per-leaf ``StableLeafPool`` is unchanged.

The ops table gains nothing for the common case and one thing for the new one:

.. code-block:: c++

   enum class PoolConcurrency : std::uint8_t {
       GraphConfined,   // default; no lock is taken on any path
       Synchronised,    // acquire and batched return take the pool's lock
   };

The policy is fixed when the pool is constructed and is a property of the pool
*instance*, not of the call site.  Per-tick code therefore never branches on
it, and a ``GraphConfined`` pool is byte-for-byte the behaviour RFC 0013
shipped.

A ``Synchronised`` pool exists for one shape: a node-owned pool whose slots are
acquired by producer threads and released by the graph.  Its contract is
precise, and it preserves the single-threaded evaluation ruling rather than
bending it:

* ``acquire`` runs on the publishing thread under the pool's lock;
* the graph thread **never** releases directly — releases accumulate in a
  node-side vector and are returned in one batch from an after-evaluation
  notification (``EngineControlView::add_after_evaluation_notification``,
  ``executor.h:197``), which takes the lock once per cycle at a defined engine
  boundary; and
* the graph's per-tick path therefore takes no pool lock at all.

That lock sits alongside the queue lock ``take_all`` already takes once per
cycle, on the same producer boundary, and outside per-tick value storage.

Ownership
---------

**Root-graph pool.**  The root graph constructs its pool during graph
construction.  Where the handle is kept is a matter of convenience rather than
of contract: the root graph's own storage and its ``GlobalState`` are equally
acceptable, because at run time a graph's ``GlobalState`` is global only to
that graph — it is the graph's own isolation copy, not a process- or
thread-scoped store.  What the contract forbids is any *third* location: no
thread-local, no process-wide static, no registry consulted by the allocating
op.

This RFC specifies ``GlobalState`` carriage, under a reserved key following the
existing convention (``type_realization.cpp:41``), because it makes the pool
reachable from a node that sees only global state and from inspection without
widening ``GraphView``.  Either location satisfies the rule; an implementation
that finds the graph's own storage simpler may use it, and the acceptance
criteria are written against the rule, not the location.The handle is an opaque scalar over a
``std::shared_ptr<ValuePool>`` — the pattern the Kafka extension already uses
for ``SubscriptionEventScheduleHandle``
(``extensions/kafka/src/detail/service_transport.h:271``).  Build-time
``shared_ptr`` is sanctioned; the per-tick path uses a raw pointer resolved
once, never the handle.

The publication point matters and is not free to choose.  ``GlobalState`` is
seeded at wiring end by *copying* the selected state into the graph, and
results copy back at run end (``global_state.h``, lifecycle ruling 2026-07-27).
A pool must therefore be created by the **running graph**, not by wiring: the
wiring-time state carries only the policy
(``set_pooled_compound_scalar_storage``), and the pool handle is written into
the graph's own isolation copy at construction and **erased before the run-end
copy back**, so no reference to a dead pool can escape into a caller's state.

**Node-local pool.**  A node that needs its own declares it as a field of its
local state (``Node::state()``, ``node.h:319``), holding the same handle scalar.
It chooses its own concurrency policy — ``Synchronised`` for the multi-producer
case — and owns the pool's lifetime, which ends with the node.  It reaches the
graph's pool through ``GraphView`` when it needs to hand a value on.

Binding
-------

An allocating operation must be handed its pool.  There are exactly two ways it
can be, and which applies is decided by whether the allocation site is named in
source.

**Explicit — the builder carries the pool.**  All new code, including
``Shared<T>``, allocates through a builder that takes a ``ValuePoolView``.
Nothing ambient is involved, and a node-local pool is expressed by handing the
node's own view to the builder it calls.  This is the whole node-local story,
and it is why ``Shared<T>`` needs no ambient channel: immutability means copy is
a retain and destroy is a release, both through the slot header, so explicit
construction is the *only* allocating operation it has.

**Implicit — the ops context reaches a binding the graph installed.**  Pooled
polymorphic compound scalars allocate inside generic value ops, whose only
per-instance channel is the ``context`` pointer.  The realized
``PolymorphicValueType`` for a pooled union is stored in a map owned by the
``TypeRealizationSnapshot`` (``type_realization.cpp:779``) and shared by every
graph using that snapshot, so its context cannot itself *be* a pool — which is
the reason the ambient channel exists at all.

The realization therefore owns one **pool binding cell** at a stable address,
and every pooled entry it creates holds a pointer to that cell.  The root
graph's pool owner **binds** itself into the cell when it is constructed and
unbinds when it is destroyed — the same object owns the pool and the binding,
so there is no state to leave behind on an unwind.  An allocating op reads the
cell through its context: one load, established once per run, at the place the
Builder already binds Schema to a concrete Plan and Ops.

The binding is exclusive, and this is the invariant the design rests on:

   A realization snapshot has at most one live root graph.

Binding a second one is refused with a diagnostic rather than silently
overwriting.  Nothing in the tree violates it — a snapshot is captured per
builder (``graph.cpp:2170``) and a nested graph deliberately reuses its root's
(``graph_wiring.cpp:2539``, ``:2746``) — and it is strictly tighter than what
the thread-local permitted, which was two root graphs sharing a snapshot on two
threads without either of them knowing.

The cost is stated plainly: the cell is mutable state on a build-time artifact,
written once per run.  It is not thread-scoped, not process-scoped, and never
consulted through a lookup.

Three consequences, all of them wanted:

* **"No pool" fails where it is caused, not where it is noticed.**  An unbound
  cell means no graph is running, which is a programming error at the call
  site; the diagnostic names the schema and the missing binding instead of
  reporting that some ambient scope was never installed.
* **Wiring-time values are inline by construction.**  A value built outside a
  graph has no pool and realizes inline.  The distinction already exists in the
  API — ``graph_type_for`` is documented as realizing "storage owned by a
  graph, including eligible pooled unions", against plain ``type_for`` — and
  this makes it structural instead of conditional.
* **RFC 0013's concurrency criterion is strengthened.**  Its acceptance
  criterion "multiple pure-C++ engines execute concurrently on separate threads
  without sharing pool state" is currently met by giving each thread its own
  ambient slot.  It is met here by there being no shared slot to contend for:
  two root graphs on two threads carry two realizations of two pools, and would
  remain correct on one thread.

Nested graphs inherit the root's binding by construction: they share its
realization, so they share its cell and never bind one of their own.  A builder
used to construct two root graphs in sequence binds, unbinds, and binds again.

Concurrency limit: one live pooled root graph per realization
-------------------------------------------------------------

Exclusivity has a consequence that must be stated rather than discovered.
``TypeRealizationSnapshot::capture`` **caches** by ``(registry generation,
options)`` and returns the same snapshot to every caller
(``type_realization.cpp:1350-1368``).  Two pooled root graphs built at the same
registry generation with the same options therefore share one realization, and
so one binding cell: the second to construct is **refused**, with a diagnostic
naming the conflict.

What still works is most of what is done:

* **Sequential runs** — bind, unbind on teardown, bind again.  A graph run
  repeatedly from one builder is unaffected.
* **Nested graphs** — they share their root's realization deliberately and
  never bind one of their own.
* **Every graph that has not opted into pooling** — no pool, no cell, no
  binding, and no behaviour change of any kind.
* **Concurrent engines whose realizations differ** — a different registry
  generation or different options gives a different snapshot and a different
  pool.

What is refused is two *concurrently live* pooled root graphs sharing a
realization.  RFC 0013 lists "multiple pure-C++ engines execute concurrently on
separate threads without sharing pool state" as an acceptance criterion, and
under the thread-local that case worked by giving each thread its own ambient
slot.  This RFC narrows it for pooled graphs: the case is refused loudly
instead of served.  That is a deliberate trade — the alternative designs below
cost considerably more — and it is reversible without changing the binding
contract.

Two follow-ups would restore it, neither needed until a real use appears:

* **Lease a realization per live pooled graph.**  Capture would hand out a
  realization whose cell is free rather than a shared one.  The cost is one
  graph-type compilation per concurrent pooled graph, plus lifetime work that
  is not incidental: value types interned from a snapshot outlive it today, and
  the process-wide graph type registry holds them, so realizations are
  effectively immortal by design.
* **Thread the pool through the allocating ops.**  Correct for any number of
  concurrent graphs, and disproportionate: it changes the signature of the ops
  vocabulary to serve three allocating entries.

Why not per-instance realized types
-----------------------------------

The obvious alternative — give each root graph its own realized pooled types so
the context *is* the pool — was tried against the build path and rejected on
evidence.

Compiled graph types are canonicalised **process-wide** by builder equivalence
(``graph_runtime_registry().make_types``, ``graph.cpp:1329``) and cached on the
builder (``:2211``).  Node types embed their realized value types, so a
per-instance realization produces per-instance node types, which miss the
canonical entry every time.  Each run would then add a permanent entry to a
process-wide registry that has no eviction — a leak proportional to runs, paid
by exactly the workloads that build many small graphs.

Recompiling the graph type per run would also discard the canonicalisation that
makes those workloads cheap, to solve a problem one pointer already solves.

What is removed
---------------

* the ``thread_local`` at ``stable_leaf_compound_scalar_storage.cpp:417``;
* ``active_compound_scalar_storage()`` and ``CompoundScalarStorageScope``,
  including their declarations in ``compound_scalar_storage.h``;
* the eight scope installations in ``graph.cpp`` listed under `Motivation`_ —
  three of which were ``pooled_start_impl``, ``pooled_stop_impl``, and
  ``pooled_evaluate_impl``, wrappers that existed only to install a scope and
  so charged a thread-local write and restore to **every graph cycle**; pooled
  graphs now run the same lifecycle functions as every other graph;
* the five scope installations in the native tests
  (``test_ts_input.cpp``, ``test_type_registry.cpp`` ×2,
  ``test_value_builder.cpp``, ``type_erasure_perf.cpp``), which bind to a
  realization instead; and
* the ambient availability condition: an allocating op now fails against its
  own binding, with a diagnostic saying no root graph is bound, rather than
  reporting that some scope was never installed.

Explicitly **not** removed, and out of scope here: the type-realization
thread-locals ``active_snapshot`` and ``graph_value_realization``
(``type_realization.cpp:39-40``).  Those are build-time channels used while a
graph constructs its nodes, not per-tick runtime state.  They are a separate
cleanup and are recorded as such rather than smuggled into this change.

Lifetime invariant
------------------

   A pooled payload never outlives the pool that owns its slot.

The root-graph pool dies with the root graph; a node-local pool dies with the
node.  A value leaving either — converted to Python, recorded, written to a
table, or copied into a caller's ``GlobalState`` — **materialises** into
independently owning storage on the way out, which is what RFC 0013 already
requires of caller-owned values and what its cross-graph rules already enforce
("pooled closed Bundle source is outside this graph snapshot",
``pooled_polymorphic_value_type.cpp:260``, ``:276``).  Making the pool
node-ownable adds one case to state: a value allocated from a node-local pool
and retained by any other node materialises, because its pool's lifetime is not
the graph's.

Compatibility, migration, and serialization
-------------------------------------------

Wire formats, value semantics, hashing, equality, ordering, and tick behaviour
are unchanged.  Graphs that do not opt into pooling are unaffected in layout
and in per-tick behaviour, as RFC 0013 requires.

``compound_scalar_storage.h`` is renamed to ``value_pool.h`` with the type
renames above.  No extension in this repository references either type, so the
migration is internal; it is nevertheless an ABI change requiring a rebuild of
downstream native extensions, and the installed-SDK consumer fixture must build
against the renamed header.

One exported function changes shape rather than name:
``retain_or_copy_pooled_compound_scalar`` takes the pool it is retaining *into*
as its first argument.  Its ambient form could only ask which thread was
calling; a payload from another pool is materialised into the target it was
given.

Python exposure is one reserved ``GlobalState`` key holding an opaque handle.
It is not part of the public Python surface and is erased before run-end copy
back.

Performance and memory
----------------------

Allocation loses a thread-local access and gains a read of a pointer already in
the ops context, so the per-tick path is strictly cheaper or identical; retain,
release, and destroy are untouched.  ``GraphConfined`` pools take no lock, and
the graph's per-tick path takes no lock even when a ``Synchronised`` pool is in
use.

Build time grows by one realization of the pooled types per root graph instead
of one per snapshot — bounded by the number of pooled schemas in the graph, and
paid once at construction.  Pool memory per root graph is unchanged.

No claim here is measured.  See `Acceptance criteria`_.

Alternatives considered
-----------------------

* **Re-root the ambient channel.**  Keep ``CompoundScalarStorageScope`` but
  source its view from the GlobalState-carried pool.  Much smaller, and it does
  move ownership where it belongs — but the invisibility, the runtime
  availability condition, and the one-pool-per-thread limit all survive, and the
  third of those is what blocks RFC 0028.
* **A process-wide static instead of a thread-local.**  Rejected: it removes
  the word ``thread_local`` and breaks RFC 0013's criterion that engines run
  concurrently on separate threads without sharing pool state.
* **Plumb an evaluation context through every value op.**  Rejected as
  disproportionate: it changes the signature of the entire ops vocabulary to
  serve three allocating entries.
* **Look the pool up from ``GlobalState`` at each allocation.**  Rejected: a
  keyed map lookup on an allocation path is the per-tick registry-lookup
  pattern that has already caused one measured regression in this repository.
  The handle is resolved once and the raw pointer carried.
* **Store the pool pointer in the value's own inline storage.**  Rejected: it
  widens every pooled holder beyond one pointer, and an empty holder being
  default-constructed still has nothing to read.
* **A stack of ambient scopes so a node can push its own pool.**  Rejected: it
  expresses two pools by making the binding depend on call depth, which is
  strictly less legible than the mechanism it replaces.

Unresolved questions
--------------------

1. Whether the exclusive-binding invariant should be enforced only in debug
   builds.  It is proposed as an unconditional check: it costs one comparison
   at graph construction, and the failure it prevents is a silent data race.
2. Whether ``Synchronised`` pools belong in this RFC at all.  The object and its
   policy are defined here; its only user is RFC 0028's producer-owned pools,
   which that RFC defers pending measurement.  Defining it here and leaving it
   unused until measured is deliberate, but it could equally move.
3. Whether a wiring-time value of a pooled schema should realize inline
   silently, or whether an explicit request for pooled storage outside a graph
   should be an error.

Whether the root pool lives on ``GlobalState`` or on the root graph's own
storage is **not** an open question: both are the running graph, both satisfy
the rule, and the choice is left to the implementation.  What is settled is
that no third location is permitted.

Acceptance criteria
-------------------

* No ``thread_local`` remains in the value-pool implementation, and no pool
  state is reachable other than through a running graph or a node — enforced by
  a source-level check alongside the existing lock-counting enforcement, so the
  channel cannot quietly return.
* Two root graphs whose realizations differ allocate into their own pools, on
  one thread and on two.
* Binding a second live root graph to one realization is refused with a
  diagnostic; binding, unbinding, and rebinding in sequence works, including
  after a failed graph construction unwinds.
* A node-local pool and its graph's pool are live simultaneously, and values
  allocated from each release to the correct owner.
* A ``Synchronised`` pool acquires off-thread, batches its returns through an
  after-evaluation notification, and is clean under ASAN and UBSAN.
* Pool lock acquisitions observed on the graph's per-tick path are zero,
  counted and asserted in the manner of
  ``RuntimeRegistrySnapshot.type_system_lock_acquisitions``.
* Every scope installation is deleted and the existing pooled native and Python
  suites pass unchanged; the four native test sites bind explicitly instead.
* A pooled value converted to Python, recorded, or copied into a caller's
  ``GlobalState`` materialises; the reserved key holds no handle after run end.
* A value of a pooled schema built outside a graph realizes inline.
* Metrics attribute pool live and reserved bytes exactly once at the root
  graph, unchanged from RFC 0013.
* The installed-SDK consumer fixture builds and runs against the renamed
  header.
* RFC 0013's own acceptance criteria continue to hold, in particular concurrent
  engines on separate threads and no per-tick scope work in graphs that have not
  opted in.

Implementation status
---------------------

None.  This RFC is ``Proposed`` and contains no implementation.

References
----------

* ``src/hgraph/types/value/impl/stable_leaf_compound_scalar_storage.cpp`` — the
  thread-local selection channel (``:417``), the no-op allocating entries and
  ``unavailable_storage()`` (``:329``), ``LeafSlotHeader`` and its owning-pool
  back-pointer (``:32``), and ``StableLeafPool``.
* ``include/hgraph/types/value/compound_scalar_storage.h`` — the owner/view/scope
  triple this RFC renames and reshapes.
* ``src/hgraph/runtime/graph.cpp`` — root ownership and nested inheritance
  (``:566-583``) and the eight scope installations.
* ``src/hgraph/types/metadata/type_realization.cpp`` — the snapshot-owned
  realized-type map (``:779``), the reserved-key convention (``:41``), and the
  build-time thread-locals left in place (``:39-40``).
* ``include/hgraph/runtime/global_state.h`` — the wiring seed and run-end copy
  back that fix where a pool may be published.
* ``include/hgraph/runtime/executor.h:197`` —
  ``add_after_evaluation_notification``, the cycle boundary for batched slot
  return.
* ``extensions/kafka/src/detail/service_transport.h:271`` — the handle-scalar
  precedent for carrying a ``shared_ptr`` owner through a value.
* :doc:`RFC 0013 <rfc_0013_pooled_polymorphic_compound_scalars>` — the pooling
  machinery, whose storage-selection mechanism this RFC supersedes.
* :doc:`RFC 0028 <rfc_0028_shared_value_representation>` — the first consumer of
  explicit pool binding and of node-owned pools.
