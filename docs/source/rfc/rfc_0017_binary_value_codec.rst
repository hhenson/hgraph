RFC 0017: Binary Value Codec and Stream Framing
===============================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-11
:Target: Value serialization, cross-process transport payloads, and storage framing

Summary
-------

A compact schema-driven **binary codec for values**, synthesized per
``ValueTypeMetaData`` and interned exactly as ``JsonConverter`` already is, plus
a minimal **stream envelope** carrying schema identity, a monotonic sequence,
and an image/delta discriminator.

The codec's unit is a ``Value``, not a time series. That is deliberate and it
is what makes this RFC small: ``ts_delta.h`` already reduces a per-cycle
time-series delta to a canonical ``Value`` over
``TSValueTypeMetaData::delta_value_schema``, and already provides the inverse.
Everything a transport or a store needs above the byte level therefore exists.
What is missing is a wire encoding worth putting on a socket, and an envelope
that lets a receiver know what it is holding and whether it missed anything.

This RFC deliberately decides **no transport**. It is the piece every candidate
transport needs, and building it first keeps that decision cheap and
reversible.

Motivation
----------

Four consumers want the same thing, and none of them can use what exists today:

* **Remote services.** RFC 0011 unified services and adaptors onto one boundary
  model, and RFC 0014 already plans the decoupled sink/source shape — an
  implementation may sink requests to an external transport and publish
  correlated responses from a push source at *zero added engine cycles*. The
  transparent local-or-remote binding is therefore already designed. The
  payload encoding is not.
* **RFC 0016 object-store persistence.** A delta log is framed deltas appended
  to a key. It needs exactly this envelope.
* **Record/replay.** The TABLE protocol is columnar and bitemporal — the right
  shape for batches of rows over time, the wrong shape for a single cycle's
  delta.
* **Cross-language and cross-process tooling**, which needs to read a stream
  without having been compiled against the schema.

The only general value serialization in the tree is ``json_codec.h``. It is
correct and it is the right thing for diagnostics, configuration, and
interop — and it is far too large and too slow to put under a per-tick payload.

Ownership boundary
------------------

This RFC owns the **encoding and the envelope**. It owns no transport, no
discovery, no connection management, and no service failure semantics.

In particular it does **not** decide between a broker (NATS, Kafka), a
brokerless transport (Aeron), or a direct connection layer. Those belong to a
later transport RFC, which will also have to define what a client observes when
a provider disappears — a question the current service contract does not
answer, because a local implementation cannot vanish mid-run.

The codec lives in core rather than in a transport extension because three of
its four consumers are core.

What already exists (and is therefore not proposed here)
--------------------------------------------------------

Recording this explicitly, because it is the reason the proposal is narrow.

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Capability
     - Where it already lives
   * - Per-cycle delta capture and apply
     - ``ts_delta.h`` — ``capture_delta`` / ``apply_delta``
   * - Whole-value (snapshot) apply
     - ``ts_delta.h`` — ``current_value_schema_compatible`` and the
       current-value apply path
   * - Canonical delta shape per time-series kind
     - ``type_registry.cpp``; see the table below
   * - Interned per-schema converter synthesis
     - ``json_codec.h`` — ``JsonConverter``, ``json_converter(meta)``
   * - Value memory layout with concrete offsets
     - ``value/`` and ``types/utils/memory_utils.h`` — ``StoragePlan``

The canonical delta shapes this codec must therefore encode are ordinary
values:

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Time-series kind
     - Canonical delta value
   * - ``TS<T>``, ``SIGNAL``, ``TSW<T>``
     - scalar
   * - ``TSS<T>``
     - ``Bundle{added: Set<T>, removed: Set<T>}``
   * - ``TSD<K,V>``
     - ``Bundle{removed: Set<K>, modified: Map<K, delta(V)>}``
       — see "strict removal" below
   * - ``TSL<C,N>``
     - ``Map<int, delta(C)>``
   * - ``TSB{f...}``
     - ``Bundle{f: delta(f)...}``, recursive in children
   * - ``REF``
     - **not encodable** — see below

``REF`` is excluded by construction, matching ``capture_delta``, which throws
``std::logic_error`` for it. A reference is a process-local binding onto
another endpoint's storage; it has no meaning in another address space. A
remote service sends values, never references. This is a property of the
model, not a limitation of the encoding, and the codec reports it the same way.

Strict removal is intent, not observation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``TSD`` row above states the **observed** delta, and it is deliberately a
two-field bundle. The runtime today builds a third field into
``delta_value_schema``:

.. code-block:: text

   Bundle{removed: Set<K>, modified: Map<K, delta(V)>, removed_strict: Set<K>}

``removed_strict`` exists only for the **apply** direction. It carries
user-authored ``REMOVE`` keys, which must raise when the key is absent, as
against the lenient ``REMOVE_IF_EXISTS`` that travels in ``removed``
(``type_registry.cpp``; the raising apply path is in ``ts_delta.cpp``). Its
only producer is the Python dict conversion in ``value_conversion.cpp``, which
populates it when a node's output dict contains the ``REMOVE`` sentinel.
Capture never produces it — ``ts_delta.cpp`` builds it as an empty set with the
comment *"captures never carry strict removals"*.

So the field exists **solely for a user returning a delta**, never for reading
one, and the reason is definitional rather than a matter of convention:

* **Reading.** A key an observation reports as removed *was there* — that is
  what "removed" means. Every observed removal is therefore already a
  ``REMOVE``; there is no ``REMOVE_IF_EXISTS`` to distinguish it from, and
  nothing for a strict variant to add.
* **Applying.** Applying that same fact to another time-series is always
  lenient. The target being replayed, replicated or seeded need not hold the
  key, so a removal that cannot find one is not an error.
* **Authoring.** Only here can strictness mean anything: a user asserting that
  the target *does* hold the key and wanting to be told when it does not.

A delta read from a ``TSD`` therefore has added, removed and updated keys and
nothing else. Two directions were being carried by one schema.

**Consequences for this RFC.** The wire delta for ``TSD`` is
``{removed, modified}``. Strict removal is not representable and must not be:
the codec transports observations between processes, and a sender expressing
authored intent is mutating a local ``TSD``, not shipping a delta. Encoding it
would also put an always-empty set in every ``TSD`` frame.

**Consequence for the runtime**, recorded here per the doc/code discipline in
``CLAUDE.md``: the two schemas should be separated rather than conflated —
``delta_value_schema`` narrowed to the observed two fields, and a distinct
mutation schema carrying ``removed_strict`` accepted by ``apply_delta`` and
produced by the Python dict conversion. That is a prerequisite for this RFC,
because the converter is synthesized *from* the schema: while the canonical
schema has three fields, a synthesized converter necessarily puts three on the
wire. It is a separate change from the codec itself and touches
``type_registry.cpp``, ``ts_delta.cpp``, ``ts_data/proxy.cpp`` and
``ts_data_slot_ops.cpp``.

The initial image
~~~~~~~~~~~~~~~~~

A delta stream is only meaningful to a receiver that already holds the state
the deltas apply to. A subscriber attaching mid-run, a store whose log must be
self-contained, and a receiver recovering from a gap all need the same thing
first: an **image** of the current state.

So the codec carries two payload shapes, not one, and they are different
schemas with different application semantics:

.. list-table::
   :header-rows: 1
   :widths: 14 27 27 32

   * - Frame
     - Schema
     - Produced by
     - Applied by
   * - ``Delta``
     - ``delta_value_schema``
     - ``capture_delta``
     - ``apply_delta`` — **merges**
   * - ``Image``
     - ``value_schema``
     - the whole current value
     - ``apply_current_value`` — **replaces**

The difference is not presentational. ``apply_current_value`` on a ``TSD``
erases keys the image does not contain (``ts_delta.cpp``), whereas a delta can
only add, update, and remove what it explicitly names. A delta therefore cannot
serve as an image except against an empty target: applied to a *stale* one it
leaves ghost keys, which is precisely the state a resync exists to repair.

This is why the resync rule below discards until an ``Image`` rather than until
"the next full-looking frame".

Two consequences for the rest of this document:

* the converter is synthesized for **both** ``value_schema`` and
  ``delta_value_schema`` of a time-series, and a bound stream binds the pair.
  This also settles ``TSW``, whose delta is the single appended element while
  its image is the window contents — no special case, just the two schemas the
  registry already computes;
* an image of a partially valid input has to represent UNSET children, which
  is exactly what the presence bitmap in the wire format provides. The two
  requirements met each other: without the bitmap, only a fully valid
  time-series could be imaged.

``capture_current_delta`` — "every currently-valid value, in delta shape" —
remains the right local mechanism for a sender that must build state from a
partially valid input, but it is not a wire ``Image``: it merges, so it cannot
repair a stale receiver.

C++ contract
------------

The converter mirrors ``JsonConverter`` deliberately — same synthesis, same
interning, same lifetime, same "capture it at node start" usage rule — so this
is the second instance of an established pattern rather than a new one.

.. code-block:: cpp

   namespace hgraph::codec
   {
       /** Interned, synthesized per ValueTypeMetaData. */
       class HGRAPH_EXPORT BinaryConverter
       {
         public:
           using WriteFn = void (*)(const BinaryConverter &, const ValueView &, ByteWriter &);
           using ReadFn  = Value (*)(const BinaryConverter &, ByteReader &);

           void  write(const ValueView &view, ByteWriter &out) const { write_(*this, view, out); }
           Value read(ByteReader &in) const { return read_(*this, in); }

           /** 64-bit structural identity; stable across processes and builds. */
           [[nodiscard]] std::uint64_t schema_id() const noexcept;

           /** True when encode/decode is a straight copy of the packed image. */
           [[nodiscard]] bool trivial_layout() const noexcept;
       };

       /** Synthesizes and caches on first use; throws for schemas with no wire form. */
       [[nodiscard]] HGRAPH_EXPORT const BinaryConverter &binary_converter(const ValueTypeMetaData *meta);
       HGRAPH_EXPORT void clear_binary_converters() noexcept;

       HGRAPH_EXPORT void  encode(const ValueView &view, ByteWriter &out);
       [[nodiscard]] HGRAPH_EXPORT Value decode(const ValueTypeMetaData *meta, ByteReader &in);
   }

``ByteWriter`` wraps a caller-owned, reusable buffer: a boundary node holds one
for the life of the graph and encoding allocates nothing. ``ByteReader`` is a
non-owning cursor over received bytes.

Like ``json_converter``, synthesis is build-time machinery and may lock;
callers capture the converter at node start and never look it up per tick.

Wire format
-----------

Canonical little-endian, packed, no interior padding.

.. list-table::
   :header-rows: 1
   :widths: 24 76

   * - ``ValueTypeKind``
     - Encoding
   * - ``Atomic``
     - Fixed-width for numeric and temporal forms; varint length + bytes for
       the two length-prefixed forms, ``Str`` and ``Bytes``. The codec defines
       its OWN tag enumeration rather than reusing
       ``JsonConverter::AtomicTag``: that taxonomy has no ``Bytes`` member,
       because JSON has no byte-string form, so borrowing it would leave an
       ordinary ``Value<Bytes>`` — the payload the Kafka extension moves —
       without a wire form.
   * - ``Tuple``, ``Bundle``
     - A presence bitmap of ``ceil(n/8)`` bytes, then the PRESENT fields in
       declaration order. **No names on the wire** — the schema supplies them.
       The bitmap is not an optimisation: fields may be UNSET rather than
       defaulted (see ``Nullable`` below), and without it a decoder can
       neither tell UNSET from a value nor find the next variable-width field
       after an omission.
   * - ``List``, ``Set``
     - varint count, then elements. Fixed-size lists omit the count.
       A ``Nullable`` list carries the same presence bitmap as a bundle,
       because its elements may be holes.
   * - ``Map``
     - varint count, then key/value pairs.
   * - ``CyclicBuffer``, ``Queue``
     - varint count, then elements in order.
   * - ``Any``
     - schema descriptor (or negotiated id) followed by the contained value.

Counts, lengths, and sequence numbers are LEB128 varints — they are almost
always small. Numeric and temporal atoms are fixed-width, because they are the
bulk of the payload and a branchy varint decode is the wrong trade there.

**The fixed-layout fast path.** During synthesis, a schema may be flagged
``trivial_layout``, making encode and decode a copy of its ``StoragePlan``
image. This is the same idea that makes SBE fast, and hgraph's ``Plan`` model
suits it unusually well because the offsets are already computed and interned.

Layout compatibility alone is *not* sufficient to enable it. The copy is only
correct when the in-memory image already IS the canonical wire image, so the
flag requires all of:

* fixed size, no indirection, and no interior padding;
* a little-endian host — on a big-endian one a padding-free numeric image is
  still host-ordered, which is precisely the canonical form the format does
  not use; and
* every constituent scalar's object representation equal to its canonical wire
  representation, admitting no padding bits or alternative encodings.

Anything else takes the field-wise path, which is always correct. The
optimisation is guarded rather than trusted: a conformance test requires the
fast path to produce **byte-identical** output to the field-wise path for every
eligible schema, so the two cannot drift.

Indexing by position, not by name
---------------------------------

Both keyed structures encode by an integer index rather than by the key or name
itself. This is where most of the size saving is, and it is also what makes a
recovered graph identical to the one that was persisted rather than merely
equivalent.

``TSD`` keys ride the slot model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``KeySlotStore`` already assigns every ``TSD`` key a slot that is stable for
the life of that ``TSD``, with a live / constructed / pending-erase lifecycle
and free-listed reuse. The wire adopts it rather than inventing a per-stream
dictionary:

* an **image** is slot-indexed, carrying ``(slot, key, value)`` for live slots
  and preserving the **holes** — reloading leaves them non-live and
  non-constructed on the free list, so the receiver reconstructs the *same*
  slot space rather than a compacted one;
* a **delta** then addresses ``modified`` and ``removed`` by ``slot`` alone, so
  a string-keyed ``TSD`` stops paying for its keys on every tick;
* a key's bytes appear only where a slot first becomes live, as a
  ``(slot, key)`` binding. That is exactly the ``TSS``/key-set shape, which is
  why the key set is the one projection that must carry value **and** slot.

Preserving holes is deliberate. Compacting on reload would produce a smaller
image and a *different* graph: slot ids are identity, and anything holding one
across a save/restore would silently rebind. The cost is that an image tracks
peak cardinality rather than current — which is what the running graph already
does, so the wire is not adding a property the runtime lacks.

This also peers with the implementation instead of translating at the boundary:
the encoder reads the slot the value already lives in, and the decoder writes
into the slot it will live in, with no re-hashing on apply.

Two ways to apply a delta
~~~~~~~~~~~~~~~~~~~~~~~~~

Slot ids index the observed delta, but they are **not** how a delta is normally
applied. Two paths, and the distinction is deliberate rather than incidental:

**By key — the default, and what ordinary code gets.** Applying a delta looks
its keys up in the target, exactly as an authored delta does. This is the only
correct choice when the target's slot layout is not known to match the source's
— a fresh ``TSD``, a replay into a new graph, a peer that never received an
image. The improvement here is that the lookup returns a **reference** to the
key rather than a copy: the keys a ``TSD`` hands out are already views into its
``KeySlotStore``, so nothing needs materialising.

**By slot — image recovery and the delta stream that follows it.** Addressing
by slot skips the lookup entirely, but it is only sound when sender and
receiver share a slot layout, which holds *only* after an image has
reconstructed one. This is not ordinary user code and the API says so: it is a
separate, explicitly named entry point rather than an overload that silently
does something different, because using it without the image/delta discipline
binds values to the wrong keys and does so quietly.

The staging matters. The by-key path with borrowed keys is correct everywhere
and can land on its own; the by-slot path needs capabilities the runtime does
not have yet — slot-addressed mutation, and placing a key into a *chosen* slot
so an image can rebuild a layout including its holes. Neither exists today:
``TSDDataMutationView`` is key-addressed, and ``KeySlotStore::insert`` takes the
next free slot.

``TSB`` fields are already positional
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A bundle is encoded in declaration order with a presence bitmap and no field
names, so it is index-keyed by construction. Nothing changes for it — but the
consequence is the same one the slot model has, and it is worth stating
plainly.

Validation is not optional
~~~~~~~~~~~~~~~~~~~~~~~~~~

Positional encodings move correctness into the schema. A ``TSB`` whose fields
were reordered, inserted into, or retyped reads a *well-formed* frame as the
wrong values rather than failing; a ``TSD`` whose key type changed rebinds
slots to different keys. Neither corrupts loudly.

So descriptor equality is checked at **every** attach point, not only at a
network handshake:

* connect and reconnect, against the peer's descriptor;
* opening a stored log, and loading a snapshot, against the reader's own
  schema.

A mismatch is an error. This is the reason the descriptor carries every
wire-affecting flag and travels with stored data rather than being reduced to a
digest: it is the artifact that makes a positional encoding safe.

Recovering a running graph
~~~~~~~~~~~~~~~~~~~~~~~~~~

Reproducible slot ids are the step that takes this beyond persisting a data
stream. If a restored ``TSD`` gives every key the slot it had, then slot-derived
identity survives the restore and a graph can be brought back up in the state it
was in, rather than in an equivalent-looking one. That is a larger piece of work
and belongs in its own RFC; it is recorded here because it is the reason to
preserve holes rather than compact, and that decision has to be made now.

That graph-level contract is proposed by
:doc:`rfc_0022_serializable_graph_manifest` and
:doc:`rfc_0023_graph_checkpoint_recovery`. This RFC remains responsible for
canonical schema descriptors, value images, and time-series delta framing.
The checkpoint RFC adds graph topology, owned endpoint metadata, schedules,
recordable state, references, and restore lifecycle; those are not extensions
to the endpoint transport frame defined here.

Schema identity
---------------

**The descriptor is the identity; the id is only a handle.** An earlier draft
made ``schema_id`` a 64-bit structural hash and treated it as the identity
itself. That is the wrong way round: it makes correctness depend on a hash
function two implementations must agree on exactly, and on that hash never
colliding.

The **canonical descriptor** is the normative artifact — a byte encoding of the
schema tree, compared for equality, never merely digested.

It is explicitly *not* derived from ``ValueTypeMetaData::name()``, which is a
registry-owned diagnostic label. Identity must be structural so that two
processes — possibly two languages — agree because their types agree, not
because their labels were spelled the same way.

The descriptor carries, per node: the kind; the atomic tag where the kind is
atomic; ``fixed_size``; field names for bundles; the ordered member table for
enums; and **every flag that changes wire semantics**. That last item was
missing from the first draft and is not optional — ``VariadicTuple`` and
``Nullable`` distinguish schemas that are otherwise identical trees, and only
the latter admits UNSET holes, so omitting the flags would let two schemas with
different encodings share an identity. ``ShapedArray`` and ``Enum`` collide the
same way.

Because the wire-affecting subset of ``ValueTypeFlags`` has to stay correct as
flags are added, a conformance test asserts that **every** enumerator is
classified as wire-affecting or not, so a new flag cannot be introduced without
deciding.

Two modes, because the consumers genuinely differ:

**Bound** (services, once a stream is established). The handshake exchanges
full descriptors, both ends verify structural equality, and each is assigned a
small **stream-local** id used in subsequent frame headers. The id is therefore
a compression of an already-agreed identity rather than a claim about it, and
needs no cross-implementation hash agreement at all. A frame whose id is not
bound to this stream is an **error**, never a coercion.

**Descriptive** (storage, tooling, replay of an unknown stream). The descriptor
is encoded ahead of the payload, so a reader never compiled against the type
can still decode it. RFC 0001's typed frame metadata is the precedent for
describing a schema in bytes.

Where a stable cross-run name is genuinely wanted — a store keying objects by
schema — it is defined as the first 8 bytes of ``SHA-256`` over the canonical
descriptor bytes, with domain separation by the format version. SHA-256 because
it is unambiguous across languages with no seed to agree on. Even then the
digest is a lookup key: the descriptor travels with the data and confirms the
match, so a collision degrades to a miss rather than to silent corruption.

Stream envelope
---------------

Framing lives **in the codec, not in the transport**. This is the load-bearing
decision of the RFC: it means correct behaviour on loss and reconnect is
written once, and a transport is not trusted to provide a guarantee it may not
have.

Descriptive frame header::

    magic u16 | version u8 | schema_id u64 | sequence u64 | flags u8   (20 bytes)

Bound frame header, after handshake, where the schema is fixed per stream::

    flags u8 | sequence varint                                          (2-9 bytes)

``flags`` carries the image/delta discriminator, a descriptor-present bit, and
reserved bits. A ``Delta`` frame's payload is on ``delta_value_schema`` and
merges; an ``Image`` frame's payload is on ``value_schema`` and replaces. A
bound stream therefore binds a **pair** of descriptors at handshake, and the
discriminator selects which applies — one id per stream would not be enough.

``sequence`` is monotonic per stream. A receiver that observes a gap must not
apply the frame: it discards until the next ``Image`` and requests one if the
stream supports it. An ``Image`` is required rather than merely a large delta
because only an image removes state the receiver holds and the sender no longer
has. That single rule is what makes an at-most-once transport
survivable — core NATS drops for a slow consumer, and a dropped delta is not
lag but silent state corruption — and it is equally what makes reconnect
correct on a transport that never drops at all.

Delivery patterns
-----------------

The two consumers index the same frames differently — a live stream by
sequence, a stored log by time — so each gets an explicit contract rather than
leaving senders to invent one.

Network streams
~~~~~~~~~~~~~~~

* **Connect** → ``Image``, then ``Delta`` frames.
* **Disconnect / reconnect** → ``Image``, then ``Delta`` frames.

The image is sent on every connection epoch, not only when a receiver reports a
gap. A reconnecting receiver may hold state from before the break that the
sender has since removed, and it cannot know what it is missing; only an image
removes what is no longer there. Making it unconditional also removes a class
of negotiation — there is no "do I need a resync?" exchange, because the answer
is always yes at connect.

Mid-connection, ``sequence`` still detects loss on a transport that can drop
(core NATS), and the recovery is the same primitive: discard until the next
``Image``, requesting one where the transport allows it.

Stored logs
~~~~~~~~~~~

Two object kinds, both time-indexed, following RFC 0016's keyed store:

**Delta objects** hold ``Delta`` frames with their engine times, ordered. Time
is the index; the codec's ``sequence`` is a live-stream concern and does not
order a log.

**Snapshot objects** hold one ``Image`` frame. They are written occasionally,
and **snapshotting is configurable** — cadence is a deployment decision, since
it trades write volume against replay cost, not a codec rule.

A snapshot's time **must coincide with a delta boundary**: the image is the
state after every delta up to and including that time. Without that alignment a
reader cannot tell which deltas the image already contains, and would either
double-apply or skip.

Reading from an arbitrary start time is then:

1. find the latest snapshot at or before the start time;
2. load it, then apply deltas in ``(snapshot_time, start_time]``;
3. continue with deltas after the start time.

Where no snapshot precedes the start time, the reader falls back to the head of
the delta log — which is why a log begins with an ``Image``, exactly as a
connection does. That keeps a log self-contained: it can be read without
depending on state established before it.

The cost of the cadence is visible in step 2: it bounds how many deltas a read
must replay before reaching the requested time. Frequent snapshots make reads
cheap and writes expensive; that is the trade a deployment tunes.

Python contract
---------------

Thin by design: ``encode``/``decode`` over registered value types, ``bytes`` in
and out, plus ``schema_id`` for a registered type. Python does not gain a
second implementation of the format; it calls the same converters.

First test bed: the Kafka extension
-----------------------------------

The existing Kafka extension (RFC 0015) is the cheapest end-to-end proving
ground, and it requires no new transport work.

``KafkaProduceRecord`` already carries ``value: Bytes``, ``key: Bytes``, and
headers, so a frame drops straight into an existing service:

* graph A captures deltas of a time series, encodes each to a frame, and
  publishes through the Kafka publish service;
* graph B subscribes, decodes, and applies through ``apply_delta``;
* the test asserts the two time series agree tick for tick.

This exercises the encoding, the envelope, schema binding, and — because
rebalances and offset resets are ordinary Kafka events — the gap-detection and
resnapshot path, against a real broker. It proves the codec without committing
to a transport, which is precisely the sequencing this RFC argues for.

Compatibility and migration
---------------------------

Additive. ``json_codec.h`` is unchanged and remains the diagnostic and interop
format. The TABLE protocol is unchanged and keeps its columnar/bitemporal job.
No existing wire format, stored artifact, or public signature changes.

The ``version`` byte carries format evolution. A reader rejects a version it
does not know rather than guessing.

Performance and memory
----------------------

* Synthesis is build-time and interned; per-tick work is fn-ptr dispatch with
  no type branching, matching every other ops table in the tree.
* Encoding allocates nothing beyond growth of a caller-owned reusable buffer.
* The fixed-layout fast path reduces eligible schemas to a copy.
* This is a boundary-node cost, not a graph-evaluation cost: nothing here runs
  on the per-tick path of ordinary nodes.

Evidence for these claims is required by the acceptance criteria below, not
asserted here.

Alternatives considered
-----------------------

**JSON (``json_codec.h``).** Already present, and kept. Too large and too slow
for a per-tick payload; every message pays field names and text conversion the
schema already knows.

**Arrow IPC.** Already linked, and right for frame-valued payloads, batches,
and columnar storage — where RFC 0016 uses it. Wrong for a small delta, where
the flatbuffers schema and record-batch header dominate the message. The two
coexist; this is not an either/or.

**SBE, Cap'n Proto, FlatBuffers, protobuf.** All assume schemas known at build
time and generate code from them. hgraph's schemas are composed at runtime
through the registry, so codegen cannot express them. SBE's *layout*
philosophy is adopted by the fast path; its toolchain is not applicable.

**MessagePack or CBOR.** Self-describing, so every message pays type tags and
field names that the bound stream already knows. The descriptive mode covers
the "receiver does not know the schema" case at a fraction of the steady-state
cost.

**Reusing the TABLE/columnar path.** Designed for batches of rows over time.
Encoding one cycle's delta as a one-row table pays schema overhead per tick.

**Putting framing in each transport.** Rejected: it would re-implement gap
detection and resync per transport, and would make the guarantee depend on
which transport a deployment chose.

Unresolved questions
--------------------


* **``Any`` in bound mode.** It carries per-instance schema identity, which a
  bound stream otherwise avoids. Descriptive-only, or a per-stream schema
  dictionary?
* **Big-endian hosts.** Proposal is a canonical little-endian wire with
  byte-swap on such a host. No big-endian platform is in CI or on the support
  matrix.
* **Handshake ownership.** Bound mode needs a schema agreement step. It may
  belong to the transport RFC rather than here.

Acceptance criteria and test plan
---------------------------------

* Round-trip equality for every ``ValueTypeKind`` and every atomic tag,
  enumerated from the REGISTRY rather than from the JSON taxonomy, so an
  atomic with no JSON form — ``Bytes`` — is covered, and a newly registered
  atomic without a binary tag fails the suite rather than passing silently.
* Round-trip of a partially valid ``Bundle`` and of a ``Nullable`` list: UNSET
  fields survive as UNSET, are distinguishable from defaulted values, and a
  variable-width field following an omission still decodes.
* Two schemas differing only by a wire-affecting flag (``VariadicTuple`` vs
  plain ``List``; ``Nullable`` vs not) produce different descriptors and do not
  satisfy each other's bound-mode check.
* Every ``ValueTypeFlags`` enumerator is classified wire-affecting or not, so
  adding a flag forces the decision.
* ``trivial_layout`` is refused for any schema whose in-memory image is not the
  canonical wire image, and the fast path is byte-identical to the field-wise
  path wherever it is enabled.
* Round-trip for every canonical delta shape in the table above, including
  nested ``TSB`` and ``TSD<K, TSB{...}>``.
* Round-trip of the ``value_schema`` image for every time-series kind,
  ``TSW`` included.
* An ``Image`` applied to a **stale** target erases what the image does not
  contain — the property a delta cannot provide, and the reason resync waits
  for one. The same case applied as a delta is asserted to leave the ghost
  entry, so the distinction is pinned by test rather than by prose.
* An image of a **partially valid** input round-trips with its UNSET children
  intact.
* A ``TSD`` reloaded from an image assigns every key the slot it held, and its
  holes come back as non-live, non-constructed free-list entries — so a delta
  addressed by slot lands on the intended key.
* A delta carries no key bytes for a key already bound; a newly live slot
  carries its ``(slot, key)`` binding exactly once.
* A descriptor mismatch is refused at every attach point — connect, reconnect,
  log open and snapshot load — including the cases a positional encoding would
  otherwise accept silently: a reordered ``TSB``, an inserted field, and a
  changed ``TSD`` key type.
* **Prerequisite:** the ``TSD`` observed delta schema is ``{removed,
  modified}``, with strict removal moved to a distinct mutation schema
  accepted by ``apply_delta``. A captured ``TSD`` delta encodes no
  ``removed_strict`` field, and a ``REMOVE``-sentinel dict from Python still
  raises on an absent key.
* ``REF`` is rejected with a clear error, matching ``capture_delta``.
* A ``schema_id`` mismatch in bound mode is an error, with no partial apply.
* A sequence gap suppresses apply and is cleared only by an ``Image``.
* A reconnecting receiver holding state the sender has since removed converges
  on the sender after the connect ``Image`` — the case an unconditional image
  exists for.
* A log read from an arbitrary start time agrees tick-for-tick with a read from
  the head, for a start time landing before, on, and after a snapshot boundary,
  and for a log with no snapshot at all.
* The fixed-layout fast path is byte-identical to the field-wise path for every
  eligible schema.
* Descriptive mode decodes a stream whose schema was never registered by the
  reader.
* Encoded size and encode/decode timing recorded against the JSON codec for a
  representative delta set, as the evidence for the performance claims.
* Kafka end-to-end: two graphs, tick-for-tick equality, including a forced
  gap.
* Python bridge round-trip for a registered type.

Implementation status
---------------------

Not started. This RFC is the first commit on its branch, per RFC 0000.

References
----------

* RFC 0001 — typed frame metadata (schema described in bytes).
* RFC 0011 — services and adaptors on one boundary model.
* RFC 0014 — keyed-service transport planning; the decoupled external
  transport shape.
* RFC 0015 — Kafka extension API; the first test bed.
* RFC 0016 — object-store frame persistence; a delta-log consumer.
* :doc:`rfc_0022_serializable_graph_manifest` — graph, binding, and run
  identity.
* :doc:`rfc_0023_graph_checkpoint_recovery` — running-graph checkpoint and
  restore semantics.
* ``include/hgraph/types/time_series/ts_delta.h`` — the delta model this codec
  encodes.
* ``include/hgraph/types/value/json_codec.h`` — the converter pattern mirrored
  here.
