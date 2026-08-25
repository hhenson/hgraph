RFC 0028: Shared Value Representation
=====================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-24
:Target: C++ value storage, graph runtime, adaptor ingress paths

Summary
-------

Add ``Shared<T>``, an **immutable** value-layer wrapper whose inline storage is
one pointer to a reference-counted, pool-backed allocation.  Copying a
``Shared<T>`` increments a count instead of deep-copying the payload; moving it
transfers the pointer as it does today.

``Shared<T>`` is the reference-counted sibling of the existing ``Owned<T>``
marker and reuses its plan shape, flag, descriptor pattern, and ops table.
Three properties make it small and safe:

* **Immutable.**  It publishes no mutation-capable operation.  A value is
  constructed complete and thereafter only read, projected, hashed, and stored.
* **Graph-thread confined.**  The reference count is non-atomic.  ``Shared<T>``
  is refused in a push-source sender schema, so a count is only ever touched by
  the single evaluation thread.
* **Pool-backed.**  Allocations come from the per-type pools of :doc:`RFC 0013
  <rfc_0013_pooled_polymorphic_compound_scalars>`, which supply slot reuse and
  a single accounting owner.

Where a value is large and retained several times — adaptor ingress being the
motivating case — retention cost drops from ``O(payload)`` to ``O(1)``.

Motivation
----------

Consider one live Kafka record reaching its subscription output after
:doc:`RFC 0027 <rfc_0027_bounded_push_source_queues>` burst ingress.  The
payload — record bytes, key bytes, and a header tuple — is materialised once
off-thread and then deep-copied on the graph thread at every retention point:

1. ``PushSourceSender::try_send`` moves the constructed value into the queue.
   No copy.
2. ``QueuePolicyStorage::take_all`` then ``make_burst`` moves each value into
   the burst tuple.  No copy.
3. ``SubscriptionEventSchedule::push(event.clone())`` — **copy**.  The burst
   tuple is owned by the push-source output and is valid only for this tick, so
   anything retained past the tick must be copied out.
4. ``with_evaluation_time`` rebuilds the envelope field by field to stamp a
   replay time — **copy**.
5. ``BundleBuilder::set("record", field.clone())`` projects the envelope into
   the public output bundle — **copy**.
6. ``TSDDataMutationView::set`` writes that bundle into the dictionary's child
   storage — **copy**.

Three to four deep copies of every payload, on the single evaluation thread,
for a value no graph node ever mutates.  The web adaptor's projection nodes
have the same shape with one fewer stage.

The copies are not incidental to the burst change; they are what retention
*means* under the current value contract.  ``Value`` owns its memory and
``clone`` is the only way to keep one.  A representation whose copy is a
reference-count increment removes stages 3, 5 and — subject to `Dictionary
child storage`_ — stage 6.

Ownership boundary
------------------

The C++ runtime owns the facility.  It is a value-layer wrapper type, so it is
visible in schemas by construction and invisible in behaviour: value semantics,
hashing, equality, comparison, ordering, and wire formats are those of the
target.  Adaptors select it in a schema declaration and otherwise write the
code they write today — ``clone`` on a shared value is a retain, and no call
site changes.

Selection: a type-level wrapper
-------------------------------

Selection must be explicit and visible in the schema.  An invisible
representation switch produces exactly the failure this RFC is trying to
remove: a performance cliff nobody can see when reading the type.

The value layer already has the precedent.  ``Owned<T>`` is declared in
``static_schema.h`` as *"one-pointer, on-demand owner for a value-layer
schema"*, carries ``ValueTypeFlags::Owned`` (*"Bundle-shaped indirection whose
inline storage is exactly one owner pointer"*), is registered through
``TypeRegistry::owned(const ValueTypeMetaData *)``, and is planned by
``OwnedValueEntry`` with ``layout.size = sizeof(OwnedAllocation *)``.

``Shared<T>`` is that pattern with two differences — copy retains, and mutation
is not offered:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Op
     - ``Owned<T>``
     - ``Shared<T>``
   * - ``construct``
     - null pointer; allocate on demand
     - unchanged
   * - ``move_construct`` / ``move_assign``
     - steal the pointer
     - unchanged
   * - ``copy_construct`` / ``copy_assign``
     - deep copy into a fresh allocation
     - **increment the count; share the pointer**
   * - ``destroy``
     - destroy payload, deallocate
     - **decrement; release the slot at zero**
   * - ``mutable_concrete_memory``, ``writable_concrete_memory``,
       ``make_mutable_range``, ``mutable_element_at``
     - present
     - **not published**
   * - ``from_python``
     - mutates the current allocation in place
     - **replaces the handle; never writes through**
   * - ``hash``, ``equals``, ``compare``, ``to_string``, ``to_python``,
       ``element_at``, ``make_range``, ``dynamic_storage_metrics``
     - delegate through the pointer
     - unchanged

Concretely:

.. code-block:: c++

   using KafkaTransportEvent =
       Bundle<"hgraph.kafka.internal::KafkaTransportEvent",
              Field<"kind", KafkaTransportEventKind>,
              Field<"record", Shared<KafkaRecord>>,   // shared at this site
              ...>;

This resolves the plan-site question directly.  The wrapper sits at the
*field*, not on the target schema, so ``KafkaRecord`` stays flat inside a
user's own Bundle while the transport envelope shares it.  No new concept is
needed to express that — the type says which site shares, and the type is what
a reader sees.

``Shared<T>`` is a distinct marker with its own ``ValueTypeFlags`` bit and its
own ``TypeRegistry`` entry point, not a policy parameter on ``Owned<T>``.  The
type surface is what a reader sees at a field, and ``Owned<T, Sharing::Shared>``
buries the distinction that matters in a defaulted argument.

The duplication this could imply is avoidable: the two markers share one plan
entry, which selects different lifecycle function pointers at construction
rather than branching per copy.

.. code-block:: c++

   plan.lifecycle = MemoryUtils::LifecycleOps{
       .construct     = &owned_default_construct,
       .destroy       = shared ? &shared_destroy        : &owned_destroy,
       .copy_construct= shared ? &shared_copy_construct : &owned_copy_construct,
       .move_construct= &owned_move_construct,
       .copy_assign   = shared ? &shared_copy_assign    : &owned_copy_assign,
       .move_assign   = &owned_move_assign,
   };

Two markers in the vocabulary, one entry in the implementation, and no branch
on the per-tick path.

One implementation trap is worth recording now.  ``is_un_named_bundle()``
currently reads ``kind == Bundle && wrapped_un_named == nullptr &&
!has(ValueTypeFlags::Owned)`` — the flag is what stops an ``Owned`` wrapper
being mistaken for a structural bundle.  A ``Shared`` flag must be excluded
from that predicate too, and from any other site testing ``Owned`` to mean
"one-pointer wrapper" rather than "uniquely owned".

Immutability is structural, not advisory
----------------------------------------

An earlier draft said shared values *should* be build-once and left
copy-on-write to keep mutation correct.  That is not sufficient.  ``Owned<T>``
publishes six mutation-capable operations, not one: ``mutable_concrete_memory``,
``writable_concrete_memory``, ``make_mutable_range``, ``mutable_element_at``,
and in-place ``from_python``, besides assignment.  ``MutableIndexedValueView``
reaches ``mutable_element_at`` directly, so a field or range write would never
pass a copy-on-write hook, and one holder could observe another holder's
payload change underneath it.

``Shared<T>`` therefore publishes **no** mutation-capable operation.  A
mutation attempt is a wiring-time or construction-time error naming the schema,
not a silent copy and not a data race.  The value is assembled through its
builder, and after ``build`` it is read-only for the rest of its life.

This has three consequences that shrink the rest of the design:

* the copy-on-write path disappears, so there is nothing to get wrong;
* RFC 0013's sticky ``unshareable`` bit — set permanently by
  ``StableLeafPool::writable`` — is unreachable, which removes the blocker that
  previously made pool backing a separate stage; and
* ``from_python`` replaces the handle rather than writing through it, so Python
  assignment cannot reach a payload another holder shares.

Keeping mutation and uniquifying at every entry point remains the general
alternative; see `Alternatives considered`_.  It is not needed for any current
use and it reintroduces every problem above.

Thread boundary
---------------

Reference counts are **non-atomic**, and no thread-safety machinery is added.
A graph and its shared values are confined to the single evaluation thread, as
the runtime's single-threaded ruling and RFC 0013 both require.

Confinement is enforced, not merely intended.  ``Shared<T>`` is **refused in a
push-source sender schema**: ``push_value_schema_acceptable`` rejects it when
the policy is constructed, so a producer can never construct, copy, or destroy
a counted handle off-thread.  Without that rule the public sender API would
allow it — ``PushSourceSender::try_send(Value value)`` takes by value, so a
producer copying an lvalue would increment a count off-thread, and two producer
threads could race on the same allocation.

Values therefore cross the boundary in their plain, unshared representation.
No copy is required to do so: ``take_all`` returns owned values and
``make_burst`` already consumes them with ``builder.push_back(std::move(value))``,
so the crossing is a **move** throughout.  The graph thread moves each value
into a shared allocation as the burst tuple is assembled; from that point every
retention is an increment.  Where a move is not available at some future
boundary, a copy there is acceptable — the cost is one materialisation that the
boundary requires anyway.

Producer-owned pools: a considered extension
--------------------------------------------

The rule above costs one materialisation per event on the graph thread.  That
can be removed by exploiting an asymmetry the burst path already has: **one
thread constructs a value and a different thread destroys it.**  A push node
could own its own pool, let the producer encode directly into it, and hand the
slot across with the value.

The governing invariant is precise and worth stating as the contract:

   A shared value is **created** on the publishing thread, and thereafter
   **counted and destroyed only on the graph thread**.

The sender is what makes that true.  Handing a value to ``try_send`` is the
last thing the producer does with it, so the count is never incremented or
decremented off-thread — the producer constructs at count one and lets go.  A
non-atomic count is then correct by construction, not by convention, and the
queue mutex already supplies the happens-before edge that publishes the
producer's payload writes to the graph thread.

Two conditions have to hold, and only the second is a real cost:

* **Handoff must be move-only.**  A producer that retains a reference after
  sending breaks the invariant by putting a live count on each thread.
  ``PushSourceSender::try_send(Value value)`` takes by value and will copy an
  lvalue, so the discipline needs a sender overload that consumes its argument
  rather than relying on callers to be careful.
* **The slot free list becomes cross-thread.**  This is the substantive one,
  and it is not a reference-counting problem.  ``acquire_slot`` pops
  ``free_slots_`` on the publishing thread while ``StableLeafPool::release``
  pushes to it on the graph thread, and those are genuinely concurrent however
  well disciplined the count is.  RFC 0013 deliberately kept the pool
  single-threaded, so this would introduce a cross-thread edge inside per-tick
  value storage.

The second cost has a clean remedy: freed slots are not returned eagerly but
accumulated on the graph side and handed back in one batch from an
**after-evaluation notification**.  ``EngineControlView::
add_after_evaluation_notification`` already provides exactly this — a one-shot
callback *"fired at the applicable root cycle boundary and drained there to
completion"*, and the C++-primary facility behind Python's
``EvaluationEngineApi.add_after_evaluation_notification``.  The push node
re-arms one each cycle in which slots were freed.

That gives slot recycling a defined engine boundary rather than an incidental
one, and reduces the producer-facing handoff from once per value to once per
cycle — at which point a plain mutex over a vector swap is negligible and sits
outside the per-tick value path, alongside the queue lock ``take_all`` already
takes each cycle.  The invariant above is preserved end to end: the count is
still only ever touched on the graph thread.

One shape question remains.  Kafka runs one consumer thread per session plus a
producer thread, so a node-owned pool would be written by several publishers.
Either the pool is bound per sending thread rather than per node, or slot
acquisition needs its own synchronisation — which would give back what the
batched return just bought.

That is an attractive optimisation and it should be measured against the
move-across-the-boundary baseline before being adopted.  It is not required for
the copy reductions this RFC is motivated by, so it is deliberately not in the
first cut.

Pool backing and accounting
---------------------------

Allocations are backed by RFC 0013's per-type ``StableLeafPool`` from the
outset.  The pool already keys on an exact ``ValueTypeRef``, already recovers
the concrete type from a payload address, and already implements retain,
release, and slot reuse over a packed 31-bit count in a ``LeafSlotHeader``.
Because ``Shared<T>`` never mutates, ``StableLeafPool::writable`` is never
called and its sticky ``unshareable`` bit is never set — the pool behaves as a
plain reference-counted arena.

Pool backing is not merely an optimisation here; it is what makes memory
accounting well defined.  ``DynamicStorageMetrics`` is ``{live_bytes,
reserved_bytes}`` with ``operator+=`` and carries no identity, so it cannot
deduplicate.  With per-value allocations there would be no owner able to report
a shared payload exactly once: reporting it through every handle
over-counts by the number of holders, and reporting zero from every handle
loses it entirely.  The pool is that owner.  RFC 0013's rule therefore carries
over intact — an individual handle reports no owned bytes, and pool live and
reserved bytes are attributed once at the root graph.

Construction-time selection
---------------------------

Deciding reference-countedness at construction, and letting it follow the value
through copies and moves, was considered as the selection rule.  It is rejected
for a structural reason rather than a matter of taste: **a plan is a static
memory layout, interned per schema and site.**  ``MemoryUtils::StoragePlan``
carries one ``size`` and one ``alignment``.  If reference-countedness were an
instance property, every site of that schema would need a layout able to hold
either inline bytes or a pointer, which means either

* a discriminated holder of ``max(inline_size, sizeof(void *))`` plus a tag,
  and a branch in *every* ops entry — hash, equals, compare, to_string, field
  access, metrics — on the per-tick path; or
* an always-pointer layout, which is not per-instance at all.  It is per-site
  selection with the tag removed from the type, i.e. the invisible variant this
  RFC rejects on its first line.

There is a second, quieter problem.  Assigning a counted instance into a field
whose plan is inline cannot carry the ref-ness across — the destination layout
has no pointer — so "ref-ness follows the value" would hold within same-plan
sites and silently degrade to a deep copy everywhere else.  That is precisely
the invisible cliff the explicit rule exists to prevent.

The insight behind the option is nevertheless correct, and ``Owned<T>`` already
implements it: ``owned_default_construct`` stores ``nullptr`` and
``ensure_allocation`` allocates on first use, so *allocation* is already a
lazy, per-instance decision inside a statically-shaped plan.  The two questions
are separable and both are wanted:

* the **type** decides representation — static, visible, one pointer at this
  site; and
* the **builder** decides identity — allocate fresh, or retain an allocation
  the caller already holds.

A builder that can retain rather than allocate is the natural home for stage 3
of `Motivation`_: the projection retains the burst element's allocation instead
of cloning its bytes.

Why not ``REF<T>``
------------------

Reusing the ``REF`` spelling was considered.  It does not *conflict*:
``REF<TSchema>`` today has only a ``schema_descriptor`` specialisation
resolving through ``TypeRegistry::ref(const TSValueTypeMetaData *)``, and there
is no ``scalar_descriptor<REF<T>>``, so the two would occupy disjoint template
lanes.  It is nevertheless rejected:

* **It means something different.**  ``REF[TS[...]]`` is a rebindable pointer
  to an *output endpoint*, with its own tick semantics — a REF ticks when its
  binding changes, and wiring reasons about it through ``contains_ref``.  A
  shared scalar has no tick semantics whatever; it is a storage representation
  with the value semantics of its target.  One spelling for both invites the
  reader to expect rebinding, observation, and binding-time resolution that do
  not exist.
* **It reads ambiguously in generic code.**  ``REF<X>`` would mean "endpoint
  reference" or "refcounted value" according to which lane ``X`` belongs to.
  Error messages and templated wiring code would have to be read twice.
* **The value lane already has its own vocabulary.**  ``Shared<T>`` beside
  ``Owned<T>`` states the distinction that actually matters — unique versus
  shared ownership of a one-pointer indirection — and needs no explanation to
  a reader who has met ``Owned``.

Dictionary child storage
------------------------

Stage 6 of `Motivation`_ — ``TSDDataMutationView::set`` copying into the
dictionary's child storage — is the least resolved part of this proposal.  For
that write to become a retain, the ``TSD``'s child *value plan* must use the
shared representation, which is ``ts_data_plan_factory`` territory rather than
``value_plan_factory``.

Two consequences need working through:

* a dictionary child is delta-tracked and may be observed by inputs after the
  producing tick, so allocation lifetime becomes tied to time-series lifetime
  rather than to a node's local retention; and
* ``TSD`` children are conventionally overwritten each tick.  Overwriting a
  child *handle* is a move or a retain, not a write *through* the handle, so it
  does not conflict with immutability — but that needs confirming against the
  dictionary's own mutation ops rather than assumed.

If it proves hard it should be split into its own RFC.  Stages 3 and 5 are
independently worth having and do not depend on it.

Compatibility and ABI
---------------------

Value semantics, hashing, equality, comparison, JSON and table formats, and
tick behaviour are unchanged; ``Shared<T>`` presents its target's read contract.
Schemas that do not use it are byte-for-byte unaffected.

Adding a ``ValueTypeFlags`` member and a registry entry point is an ABI change
for downstream extensions and requires a rebuild.  The installed-SDK consumer
fixture must cover a ``Shared<T>`` value crossing the extension boundary.

Python exposure follows ``Owned<T>`` for reads: the wrapper is transparent and
a shared value converts as its target.  Python assignment into a shared field
replaces the handle rather than mutating the payload.

Performance and memory
----------------------

Expected: retention drops from ``O(payload)`` to ``O(1)`` on the graph thread,
at the cost of one pointer indirection per read and a 16-byte
``LeafSlotHeader`` per live allocation.  For adaptor ingress — kilobyte
payloads retained three or four times — the trade is strongly favourable; for
small flat values it is not, which is what explicit selection exists to keep
visible.

No claim here is measured yet.  See `Acceptance criteria`_.

Alternatives considered
-----------------------

* **Mutable ``Shared<T>`` with uniquify-at-every-entry-point.**  Rejected for
  now.  It requires every one of the six mutation-capable ops to uniquify
  before writing, reintroduces RFC 0013's sticky ``unshareable`` bit and the
  permanent-copy behaviour that follows it, and needs field, range, and
  Python-originated mutation tests that immutability makes unnecessary.  No
  current use needs it.
* **Atomic reference counts.**  Rejected.  Atomics on the per-tick value path
  are banned by the single-threaded evaluation ruling, for the reason recorded
  in ``compact_storage.h`` and in RFC 0013.  Confinement plus a move across the
  boundary achieves the same result at no cost.
* **``REF<T>`` as the spelling.**  Rejected — see `Why not REF<T>`_.
* **Construction-time reference-countedness.**  Rejected as the representation
  rule — see `Construction-time selection`_ — and partially adopted as the
  builder's identity decision.
* **Time-series references (REF) as the mechanism.**  A
  ``TimeSeriesReference`` shares an output *endpoint*, not a *value*, and
  resolves at binding time.  Burst elements have no stable source output — they
  are transient, redistributed across keys, and released across several ticks —
  so REF is the wrong granularity.  It remains right where a whole time series
  is passed through.
* **Per-tick arena with bulk release.**  Simpler than counting: share
  everything for the tick, drop the arena at the end.  Rejected because
  retention explicitly outlives the tick — both adaptors' spill schedules and
  Kafka's timestamp-ordered replay deque hold values across cycles.
* **``std::shared_ptr`` payloads.**  Rejected: atomic counting, per-value
  control-block allocation, and no pooling.
* **Interning by content.**  Rejected on RFC 0013's grounds: lookup, collision
  handling, and mutation invalidation add cost unrelated to removing retention
  copies, and ingress payloads are rarely equal.
* **Per-value allocations without a pool.**  Rejected: no owner can report a
  shared payload exactly once, so memory accounting is either over-counted or
  lost.  See `Pool backing and accounting`_.

Unresolved questions
--------------------

1. Whether the burst tuple should hold shared allocations, or whether each
   projection should share at its own retention point.  Sharing at tuple
   assembly is cheaper but makes the push-source policy aware of a value
   representation.
2. Whether to adopt producer-owned pools with batched slot return (see
   `Producer-owned pools: a considered extension`_) once the baseline is
   measured.  It removes the last materialisation but needs a move-only sender
   contract and one pool per sending thread.
3. Whether dictionary child storage can participate at all (see `Dictionary
   child storage`_), or whether that must be a separate RFC.
4. Whether Python-owned structured scalars (:doc:`RFC 0004
   <rfc_0004_python_owned_structured_scalars>`) interact, since they already
   carry an indirect payload.

Acceptance criteria
-------------------

* A copies-per-record benchmark on the Kafka and web ingress paths, before and
  after, with allocation counts and p50/p99 latency.  RFC 0027 already lists
  equivalent measurements as outstanding; they should be gathered together.
* Schemas not using ``Shared<T>`` show no change in layout or per-tick
  allocation counts.
* Retention of a shared value across ticks, its release, and slot reuse, all
  proven under ASAN and UBSAN.
* Every mutation-capable entry point is refused on a ``Shared<T>`` value with a
  diagnostic naming the schema, covered for concrete projection, field access,
  mutable range, and Python assignment — not concrete projection alone.
* Declaring ``Shared<T>`` in a push-source sender schema is refused when the
  policy is constructed, and the refusal is tested for the queue, burst, and
  conflating policies.
* Storage metrics attribute a payload held by several holders exactly once.
* ``Shared<T>`` is not reported as a structural bundle by
  ``is_un_named_bundle()`` or any other ``Owned``-flag predicate.
* An installed-SDK fixture retains and releases a ``Shared<T>`` value from a
  separately built extension.
* Both adaptors' existing behavioural suites pass unchanged — the
  representation is invisible to their tests.

Implementation status
---------------------

None.  This RFC is ``Proposed`` and contains no implementation.

References
----------

* ``include/hgraph/types/static_schema.h`` — the ``Owned<T>`` marker and its
  ``scalar_descriptor``; the ``REF<TSchema>`` marker and its
  ``schema_descriptor``, showing the two lanes.
* ``src/hgraph/types/metadata/value_plan_factory.cpp`` — ``OwnedAllocation``,
  ``OwnedValueEntry``, its mutation-capable ops, and the copy/move ops this
  proposal varies.
* ``include/hgraph/types/metadata/value_type_meta_data.h`` —
  ``ValueTypeFlags::Owned``.
* ``include/hgraph/types/storage_metrics.h`` — ``DynamicStorageMetrics``, which
  carries no identity and therefore cannot deduplicate.
* ``include/hgraph/runtime/executor.h`` —
  ``EngineControlView::add_after_evaluation_notification``, the root
  cycle-boundary hook proposed for batched slot handback.
* ``src/hgraph/runtime/push_source_node.cpp`` —
  ``push_value_schema_acceptable``, the sender-schema enforcement point, and
  ``make_burst``, the graph-thread sharing point.
* :doc:`RFC 0013 <rfc_0013_pooled_polymorphic_compound_scalars>` — the pooling
  machinery, its ``LeafSlotHeader``, and the sticky ``unshareable`` bit.
* :doc:`RFC 0027 <rfc_0027_bounded_push_source_queues>` — burst ingress, whose
  retention copies motivate this work.
* :doc:`RFC 0015 <rfc_0015_kafka_extension_api>` and :doc:`RFC 0024
  <rfc_0024_web_extension_api>` — the adaptors paying the copies.
