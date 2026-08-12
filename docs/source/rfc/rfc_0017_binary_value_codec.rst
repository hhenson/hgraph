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

* **Image cadence in a stored delta log.** A log needs a leading ``Image``, and
  without periodic ones a replay walks from the beginning. The cadence is a
  deployment policy rather than a codec rule, but the store (RFC 0016) needs
  somewhere to express it.
* **``Any`` in bound mode.** It carries per-instance schema identity, which a
  bound stream otherwise avoids. Descriptive-only, or a per-stream schema
  dictionary?
* **Key dictionaries.** A ``TSD`` with string keys repeats them every frame. A
  per-stream dictionary is an obvious win and an obvious complication; propose
  deferring until measured.
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
* **Prerequisite:** the ``TSD`` observed delta schema is ``{removed,
  modified}``, with strict removal moved to a distinct mutation schema
  accepted by ``apply_delta``. A captured ``TSD`` delta encodes no
  ``removed_strict`` field, and a ``REMOVE``-sentinel dict from Python still
  raises on an absent key.
* ``REF`` is rejected with a clear error, matching ``capture_delta``.
* A ``schema_id`` mismatch in bound mode is an error, with no partial apply.
* A sequence gap suppresses apply and is cleared only by an ``Image``.
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
* ``include/hgraph/types/time_series/ts_delta.h`` — the delta model this codec
  encodes.
* ``include/hgraph/types/value/json_codec.h`` — the converter pattern mirrored
  here.
