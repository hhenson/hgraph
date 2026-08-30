RFC 0030: Typed Value Persistence and Pluggable Codecs
======================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-30
:Target: ``hgraph-persistence`` stores, extension metadata persistence, Fabric

Summary
-------

Add ``store::ValueStore`` to ``hgraph-persistence``: the typed twin of
``store::FrameStore``, carrying declared value schemas the way ``FrameStore``
carries tabular data. Its encoding is a **named, registered codec** rather than
a fixed format, selected by a store-level default and overridable per call. JSON
is the required baseline and the initial default; RFC 0017's binary codec,
protobuf, and avro register the same way without touching a caller.

Objects written through the store carry a minimal envelope naming the codec, so
a read resolves its decoder from the stored bytes rather than from out-of-band
agreement. Without that, a per-call override would be unreadable.

Motivation
----------

``hgraph-persistence`` publishes one typed store over its byte store.
``FrameStore`` takes a ``Frame`` and owns Arrow, Parquet, and compression, so an
extension persisting tabular data writes no serialization code. There is no
equivalent for a declared value schema, and ``ObjectStore`` takes only
``std::span<const std::byte>``.

The cost of that gap is visible in Fabric. ``DataRevision`` is already a
``Bundle``::

    using DataRevision =
        Bundle<"hgraph.fabric::DataRevision", Field<"format_version", Int>,
               Field<"data_id", Str>, Field<"revision", Int>,
               Field<"output_version", Int>,
               Field<"dependencies", HomogeneousTuple<DataDependency>>,
               Field<"self_predecessor", Int>, Field<"as_of", DateTime>>;

and the core already interns a ``JsonConverter`` per ``ValueTypeMetaData``.
Nonetheless ``extensions/fabric/src/metadata_codec.cpp`` carries 321 lines of
hand-written canonical binary encoding — big-endian integers, fixed field order,
UTF-8 validation, bounds checks, trailing-byte rejection — for a type the
runtime can already serialize in one call. Fabric did not choose a format
because it wanted one; it wrote a codec because the shape it needed to persist
had nowhere to go.

That cost is per extension. The next extension needing to store a struct
repeats it, and each repetition is a separate format to version, test, and
migrate. RFC 0025 anticipated this explicitly, recording a larger codec
extraction as needing "its own evidence and RFC". This is that RFC.

The evidence that the swap is safe is narrow and worth stating: nothing in
Fabric hashes or byte-compares an encoded revision. Concurrency uses the object
store's ``version_token`` through ``compare_exchange_ref``
(``publication.cpp:219``, ``resolution.cpp:197``). The canonical-byte property
of the hand-written envelope is therefore not load-bearing, and a format whose
byte layout differs run to run — as JSON's may — costs nothing that is
currently relied upon.

Codec contract
--------------

A codec is an ops table, following the runtime's established convention: a
struct of function pointers whose first parameter is the implementation
context. It is registered under a stable name.

.. code-block:: cpp

    struct ValueCodecOps
    {
        /** Encode ``value`` onto ``out``. */
        void (*encode)(void *context, const ValueView &value, ObjectBytes &out);

        /** Decode ``encoded`` as ``schema``. */
        Value (*decode)(void *context, const ValueTypeMetaData *schema,
                        std::span<const std::byte> encoded);
    };

    void register_value_codec(std::string_view name, std::shared_ptr<void> context,
                              const ValueCodecOps &ops);

Registration is build-time machinery, not a per-tick path, so guarding the
registry with a counted ``TypeSystemMutex`` is sanctioned by the single-threaded
evaluation ruling rather than a departure from it. Codecs are resolved once when
a store is constructed or a call names one, never per value.

The baseline codec is ``"json"``, implemented over the core's interned
``JsonConverter`` — ``to_json_string`` and ``from_json_string``. It is required:
a build without it is not a conforming persistence build.

Selection and self-description
------------------------------

Two levels, as requested:

* **Store default.** ``ValueStoreConfig::codec`` names the codec every write
  uses unless overridden. Unset means ``"json"``.
* **Per call.** ``write(key, value, codec_name)`` overrides for one object,
  which is what makes a mixed store possible — small metadata as JSON, a large
  retained record as binary — without splitting the store.

Reads take no codec. ``ObjectBytes`` is ``std::vector<std::byte>`` and the
object store persists no content type, so a decoder cannot be inferred from the
storage layer and must not be supplied by the caller: a per-call override would
then be silently unreadable by any reader that guessed differently.

``ValueStore`` therefore writes a minimal envelope ahead of the payload:

.. code-block:: text

    "HGV1"  u8:name_length  name_bytes  payload...

Four magic bytes, one length, the codec name, then the codec's own output. It
records exactly one fact — which decoder to use — and it is written once in the
library rather than per extension.

This is deliberately the *only* framing this RFC adds, and the distinction from
what it removes matters. The Fabric envelope encoded a domain type: field order,
integer widths, dependency canonicalisation, all of which had to change whenever
``DataRevision`` changed. This envelope is independent of every schema and every
codec; it names a decoder and stops. RFC 0017 proposes a richer stream envelope
carrying schema identity and sequence for transports, and a future codec is free
to place that inside its payload without this envelope changing.

Store contract
--------------

.. code-block:: cpp

    class ValueStore final
    {
      public:
        void write(std::string_view key, const ValueView &value,
                   std::optional<std::string_view> codec = {});

        [[nodiscard]] Value read(std::string_view key,
                                 const ValueTypeMetaData *schema);

        [[nodiscard]] std::optional<Value>
        try_read(std::string_view key, const ValueTypeMetaData *schema);

        [[nodiscard]] CompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {});
    };

``compare_exchange`` forwards the object store's version token unchanged.
Concurrency control stays a storage property; this store adds none.

Non-goals
---------

* **Key construction and ordering.** Fabric's
  ``encode_fabric_ordinal`` produces fixed-width zero-padded decimal so that
  keys sort lexicographically and prefix listing returns revisions in order.
  That is encoding for *ordering*, which no payload codec can serve, and the key
  layout is the extension's schema. Only the generic primitives — base64url,
  fixed-width ordinals — belong beside ``require_valid_key()``.
* **Frames.** Tabular data continues through ``FrameStore``. A single struct
  wrapped as a one-row table would pay Arrow's per-object overhead and lose the
  natural nesting of ``HomogeneousTuple<DataDependency>``.
* **Transport framing.** RFC 0017 owns stream envelopes. This RFC persists
  objects.
* **A canonical-bytes guarantee.** No codec is required to be deterministic
  byte-for-byte. Any future caller needing content addressing must say so, and
  should register a codec that promises it rather than assume one.

Alternatives considered
-----------------------

* **Call the core JSON codec directly from each extension** — rejected as an
  end state, accepted as a first step. It deletes the same 321 lines without a
  new API, but leaves every extension owning a format decision and a media type,
  so the next one repeats the work. It is the natural first commit of the
  implementation plan below.
* **Persist values as one-row Arrow through FrameStore** — rejected: reuses
  existing machinery at the cost of representing a struct as a table, with
  per-object overhead on the metadata path.
* **Add a content type to ObjectStore** — rejected for now, and the closer
  call. It would make objects self-describing without a new envelope, and S3
  carries content types natively. But it changes the ops table every backend
  implements, and memory and local backends would have to store and return a
  field they otherwise have no use for. The envelope keeps the change inside
  one library. If a content type is added later for other reasons, the envelope
  becomes redundant and can be retired behind the same read path.
* **Codec as a template parameter on the store** — rejected: it moves the
  choice to compile time, which defeats a per-call override and forces every
  caller to name a format it should not care about.

Migration
---------

Fabric is the first consumer and the proof.

``DataRevision`` carries ``format_version`` as its first field, and the two
formats are distinguishable without it: the new envelope begins ``HGV1`` and
the legacy envelope begins with its version byte. The read path can therefore
accept both with a four-byte check, so there is no flag day and no offline
migration.

* Write the new format from the first release that has ``ValueStore``.
* Keep ``decode_revision`` as a read-only legacy path for one release.
* Retire it when no reachable store predates the change.

Seven Fabric test files exercise the current format. Those asserting round-trip
behaviour port unchanged. Any asserting byte layout should be deleted rather
than rewritten: they test a codec that is being removed, and the replacement's
byte layout is the library's business, covered by the library's own tests.

``resolution_perf`` already exists and should answer whether the packed envelope
was buying anything measurable on the metadata path, rather than the question
being settled by argument. Metadata objects are small and one is written per
publication, against a frame write and a broker round trip; the expectation is
that the difference does not register.

Implementation plan
-------------------

Each step is independently reviewable and leaves the tree green.

1. ``ValueCodecOps``, the registry, and the ``"json"`` codec over
   ``JsonConverter``, with round-trip tests across the schema kinds the core
   codec supports.
2. ``ValueStore`` over ``ObjectStore``: envelope, default selection, per-call
   override, ``compare_exchange``. Tests covering a mixed-codec store and an
   unknown codec name failing closed on read.
3. Fabric writes and reads through ``ValueStore``; ``metadata_codec.cpp``
   reduced to the legacy decoder.
4. Generic key primitives moved beside ``require_valid_key()``; Fabric keeps its
   key layout.
5. Legacy decoder removed once the compatibility window closes.

Steps 1 and 2 are additive and land before any Fabric change, so the new surface
is exercised by its own tests before it carries a consumer.
