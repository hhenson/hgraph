RFC 0028: Shared Value Representation
=====================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-24
:Target: C++ value storage, graph runtime, adaptor ingress paths

Summary
-------

Add ``Shared<T>``, a value-layer wrapper whose inline storage is one pointer to
a reference-counted allocation.  Copying a ``Shared<T>`` increments a count
instead of deep-copying the payload; moving it transfers the pointer as it does
today.

``Shared<T>`` is the reference-counted sibling of the existing ``Owned<T>``
marker, and reuses its plan shape, its flag, its descriptor pattern, and its
ops table.  Where the value is large and retained several times — adaptor
ingress being the motivating case — retention cost drops from ``O(payload)`` to
``O(1)``.

A second stage backs ``Shared<T>`` allocations with the per-type pools from
:doc:`RFC 0013 <rfc_0013_pooled_polymorphic_compound_scalars>` so slots are
reused rather than malloc'd.  The two stages are independent and the first is
useful alone.

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
child storage`_ — stage 6, leaving one materialisation at the thread boundary
that must exist anyway.

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

``Shared<T>`` is that pattern with one behavioural difference:

.. list-table::
   :header-rows: 1
   :widths: 26 37 37

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
     - ``copy_owned`` — deep copy into a fresh allocation
     - **increment the count; share the pointer**
   * - ``destroy``
     - destroy payload, deallocate
     - **decrement; destroy and deallocate at zero**
   * - ``writable_concrete_memory``
     - already in the ops table
     - copy-on-write when the count exceeds one

Everything else — the interned plan, the indexed ops, hashing, comparison,
Python conversion, ``dynamic_storage_metrics`` — is unchanged, because those
already delegate to the target through the allocation pointer.

``Shared<T>`` is a distinct marker with its own ``ValueTypeFlags`` bit and its
own ``TypeRegistry`` entry point, not a policy parameter on ``Owned<T>``.  The
type surface is what a reader sees at a field, and ``Owned<T, Sharing::Shared>``
buries the distinction that matters in a defaulted argument.

The duplication this could imply is avoidable without compromising that: the
two markers share one plan entry, which selects different lifecycle function
pointers at construction rather than branching per copy.

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

Construction-time selection
---------------------------

Deciding reference-countedness at construction, and letting it follow the value
through copies and moves, was the other candidate.  It is rejected as the
*representation* rule, for a reason that is structural rather than a matter of
taste: **a plan is a static memory layout, interned per schema and site.**
``MemoryUtils::StoragePlan`` carries one ``size`` and one ``alignment``.  If
reference-countedness were an instance property, every site of that schema
would need a layout able to hold either inline bytes or a pointer, which means
either

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

Eligibility: build-once values
------------------------------

``Shared<T>`` should be selected for values that are constructed complete and
thereafter only read, projected, hashed, and stored.  Adaptor ingress
envelopes, broker records, and inbound protocol frames all have that shape.

Copy-on-write through ``writable_concrete_memory`` keeps mutation *correct* for
any value, but it does not keep it *fast*: a value written on every tick copies
on every tick and is worse than the inline representation it replaced.  Stage 2
sharpens this — see `Pool backing`_ — so the guidance is worth stating in the
authoring documentation rather than enforcing in the type system.

Pool backing
------------

Stage 1 needs no pool: ``allocate_owned`` already places a header
(``OwnedAllocation``) ahead of the payload, and a reference count goes in that
header.  The count is free where the payload's alignment leaves padding — the
payload offset is ``sizeof(OwnedAllocation)`` rounded up to the payload's
alignment, so a 16-byte-aligned payload already has eight spare bytes — and
costs eight bytes otherwise.

Stage 2 replaces the per-value ``allocate_storage`` with RFC 0013's per-type
``StableLeafPool``, which already keys on an exact ``ValueTypeRef``, already
recovers the concrete type from a payload address, and already implements
retain / release / copy-on-write over a packed 31-bit count.  That buys slot
reuse and removes malloc from the ingress path.

One RFC 0013 behaviour must be revisited before stage 2:
``StableLeafPool::writable`` sets its ``unshareable`` bit **permanently**, so a
slot written through once never becomes shareable again even after its count
returns to one.  That is correct and conservative for a closed union whose
values are short-lived, but for a long-lived shared value it converts a single
mutation into permanent copy-on-every-retain.  Stage 2 must either restrict
pool backing to build-once schemas or track outstanding writable projections so
the bit can clear.

Thread boundary
---------------

RFC 0013's rule holds unchanged and is load-bearing: reference counts are
non-atomic because a graph and its values are confined to one execution thread,
and off-thread producers materialise values rather than sharing them.

The burst path fits without amendment.  ``QueuePolicyStorage::take_all`` drains
under the queue mutex and returns owned values; ``make_burst`` then builds the
tuple **outside the lock, on the evaluation thread**.  That call site is
already a legal sharing point: values move from the queue into shared
allocations as the tuple is assembled, and every retention downstream is an
increment.  The highest-leverage insertion point already exists, already runs
on the correct thread, and already receives values by move.

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
* ``TSD`` children are conventionally overwritten each tick — though
  overwriting a child *handle* is not writing *through* it, and that
  distinction may be sufficient.

If it proves hard it should be split into its own RFC.  Stages 3 and 5 are
independently worth having and do not depend on it.

Compatibility and ABI
---------------------

Value semantics, hashing, equality, comparison, JSON and table formats, and
tick behaviour are unchanged; ``Shared<T>`` presents its target's contract.
Schemas that do not use it are byte-for-byte unaffected.

Adding a ``ValueTypeFlags`` member and a registry entry point is an ABI change
for downstream extensions and requires a rebuild.  The installed-SDK consumer
fixture must cover a ``Shared<T>`` value crossing the extension boundary.

Python exposure follows ``Owned<T>``: the wrapper is transparent, and a shared
value converts as its target.

Performance and memory
----------------------

Expected: retention drops from ``O(payload)`` to ``O(1)`` on the graph thread,
at the cost of one pointer indirection per read and a header per live
allocation.  For adaptor ingress — kilobyte payloads retained three or four
times — the trade is strongly favourable; for small flat values it is not,
which is what the eligibility guidance exists to prevent.

RFC 0013's accounting rule carries over and matters: an individual handle
reports no owned bytes and the allocation is attributed once, so a shared
payload is never counted once per holder.

No claim here is measured yet.  See `Acceptance criteria`_.

Alternatives considered
-----------------------

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
* **``std::shared_ptr`` payloads.**  Rejected: atomic reference counting on the
  per-tick value path is banned by the single-threaded evaluation ruling, for
  the reason recorded in ``compact_storage.h`` and in RFC 0013.
* **Interning by content.**  Rejected on RFC 0013's grounds: lookup, collision
  handling, and mutation invalidation add cost unrelated to removing retention
  copies, and ingress payloads are rarely equal.
* **Atomic counts so producers can share.**  Rejected.  The producer copy is
  the one copy a thread boundary requires; making the count atomic to remove it
  would put atomics on every graph-thread retain to save one off-thread
  materialisation.

Unresolved questions
--------------------

1. Whether the burst tuple should hold shared allocations, or whether each
   projection should share at its own retention point.  Sharing at tuple
   assembly is cheaper but makes the push-source policy aware of a value
   representation.
2. Whether dictionary child storage can participate at all (see `Dictionary
   child storage`_), or whether that must be a separate RFC.
3. Whether stage 2 restricts pool backing to build-once schemas or tracks
   outstanding writable projections (see `Pool backing`_).
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
* Retention of a shared value across ticks, its release, and — for stage 2 —
  slot reuse, all proven under ASAN and UBSAN.
* A writable projection on a shared value with a count above one copies, and
  leaves the original observable to its other holders.
* Storage metrics do not double-count a payload held by several holders.
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
  ``OwnedValueEntry``, and the copy/move ops this proposal varies.
* ``include/hgraph/types/metadata/value_type_meta_data.h`` —
  ``ValueTypeFlags::Owned``.
* :doc:`RFC 0013 <rfc_0013_pooled_polymorphic_compound_scalars>` — the pooling
  machinery behind stage 2, and the sticky ``unshareable`` bit.
* :doc:`RFC 0027 <rfc_0027_bounded_push_source_queues>` — burst ingress, whose
  retention copies motivate this work.
* :doc:`RFC 0015 <rfc_0015_kafka_extension_api>` and :doc:`RFC 0024
  <rfc_0024_web_extension_api>` — the adaptors paying the copies.
