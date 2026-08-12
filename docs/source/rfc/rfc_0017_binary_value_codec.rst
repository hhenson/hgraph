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
and a snapshot/delta discriminator.

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

So the field expresses an **expectation the author is asserting about the
target**, not something that happened to a time series. A delta *read from* a
``TSD`` has added, removed and updated keys and nothing else; "strict" is not a
property an observation can have. Two directions are being carried by one
schema.

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
       ``Str``/``Bytes``. The atomic taxonomy is ``JsonConverter::AtomicTag``,
       reused rather than re-derived.
   * - ``Tuple``, ``Bundle``
     - Fields in declaration order. **No names on the wire** — the schema
       supplies them.
   * - ``List``, ``Set``
     - varint count, then elements. Fixed-size lists omit the count.
   * - ``Map``
     - varint count, then key/value pairs.
   * - ``CyclicBuffer``, ``Queue``
     - varint count, then elements in order.
   * - ``Any``
     - schema descriptor (or negotiated id) followed by the contained value.

Counts, lengths, and sequence numbers are LEB128 varints — they are almost
always small. Numeric and temporal atoms are fixed-width, because they are the
bulk of the payload and a branchy varint decode is the wrong trade there.

**The fixed-layout fast path.** During synthesis, a schema whose ``StoragePlan``
image is fixed-size, free of indirection, and free of interior padding is
flagged ``trivial_layout``; encode and decode become a copy of that image. This
is the same idea that makes SBE fast, and hgraph's ``Plan`` model suits it
unusually well because the offsets are already computed and interned.

The optimization is guarded rather than trusted: a conformance test requires
the fast path to produce **byte-identical** output to the field-wise path for
every eligible schema, so the two cannot drift.

Schema identity
---------------

``schema_id`` is a 64-bit hash over a **canonical structural descriptor**: the
kind tree, atomic tags, field names, fixed sizes, and nothing else.

It is explicitly *not* derived from ``ValueTypeMetaData::name()``, which is a
registry-owned diagnostic label. Identity must be structural so that two
processes — possibly two languages — agree because their types agree, not
because their labels were spelled the same way.

Two modes, because the consumers genuinely differ:

**Bound** (services, once a stream is established). The schema is agreed at
handshake. Frames carry no descriptor, and a received ``schema_id`` that does
not match the stream's is an **error**, never a coercion.

**Descriptive** (storage, tooling, replay of an unknown stream). A
``SchemaDescriptor`` is encoded ahead of the payload, so a reader that was
never compiled against the type can still decode it. RFC 0001's typed frame
metadata is the precedent for describing a schema in bytes.

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

``flags`` carries the snapshot/delta discriminator, a descriptor-present bit,
and reserved bits. A **delta** frame's payload is on ``delta_value_schema``; a
**snapshot** frame's payload is on the value schema.

``sequence`` is monotonic per stream. A receiver that observes a gap must not
apply the frame: it discards until the next snapshot and requests one if the
stream supports it. That single rule is what makes an at-most-once transport
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

* **TSW snapshot shape.** The delta is a scalar, but a snapshot is the window
  contents. The current-value path exists; the wire shape needs specifying.
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

* Round-trip equality for every ``ValueTypeKind`` and every
  ``JsonConverter::AtomicTag`` form.
* Round-trip for every canonical delta shape in the table above, including
  nested ``TSB`` and ``TSD<K, TSB{...}>``.
* **Prerequisite:** the ``TSD`` observed delta schema is ``{removed,
  modified}``, with strict removal moved to a distinct mutation schema
  accepted by ``apply_delta``. A captured ``TSD`` delta encodes no
  ``removed_strict`` field, and a ``REMOVE``-sentinel dict from Python still
  raises on an absent key.
* ``REF`` is rejected with a clear error, matching ``capture_delta``.
* A ``schema_id`` mismatch in bound mode is an error, with no partial apply.
* A sequence gap suppresses apply and is cleared only by a snapshot.
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
