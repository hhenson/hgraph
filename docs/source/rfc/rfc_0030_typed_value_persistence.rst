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

**Stored bytes are exactly the codec's output.** A JSON object is a JSON
document a text editor opens and ``jq`` reads; an Arrow object is a file polars
loads directly. The store adds no header, framing, or trailer, and it does not
alter the key: the caller names its own objects, extension included if it wants
one.

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
evaluation ruling rather than a departure from it. A store resolves its default
codec once, at construction, and keeps it: an ordinary call never reaches the
registry, and only an explicit per-call override looks one up by name.

**Known limitation: per-value converter resolution.** The json codec is
implemented over ``to_json_string``/``from_json_string``, and those resolve the
interned ``JsonConverter`` on every call, taking a ``TypeSystemRecursiveMutex``
to do it. ``json_converter()`` documents the contract it expects instead —
"nodes resolve their converter in ``start`` and carry it in node State" — so a
caller encoding during evaluation, as Fabric's publisher does, takes a lock per
value. The store no longer contributes to that, and the codec registry no longer
does either, but the converter lookup remains and is a real departure from the
single-threaded evaluation ruling for any per-tick user of this store.

Closing it needs a way for a codec to bind a schema once and carry the result,
which is an addition to ``ValueCodecOps`` rather than a change to any caller.
That is deliberately left to a follow-up: the shape of the binding handle
deserves its own design pass, and inventing it here would put an unreviewed API
on the critical path of removing Fabric's codec. It is recorded so it is not
rediscovered as a mystery regression.

The baseline codec is ``"json"``, implemented over the core's interned
``JsonConverter`` — ``to_json_string`` and ``from_json_string``. It is
required: a build without it is not a conforming persistence build.

Selection
---------

The codec is **configuration**, at two levels:

* **Store default.** ``ValueStoreConfig::codec``, unset meaning ``"json"``.
* **Per call.** An optional codec argument on ``write``, ``read``,
  ``try_read``, ``try_read_versioned`` and ``compare_exchange``, which is what
  makes a mixed store possible -- small metadata as JSON beside a large record
  in a binary format -- without splitting the store.

It is never inferred. A read decodes with the codec the caller configured or
named, so reading an object with a codec it was not written with is a
configuration error and surfaces as a decode failure. That is the right
outcome: the alternative is a store that guesses, and a guess that succeeds by
accident is worse than a failure.

Keys are passed through verbatim. A caller that wants ``records/alpha.json``
writes that key and gets a file of that name; one whose keys are structured --
parsed for an ordinal, range-scanned by prefix, compared lexicographically --
keeps them exactly as they are. Fabric's revision and as-of keys are the second
kind, and an earlier draft of this RFC had the store append an extension to
them, which broke the as-of scan in two ways at once: the ordinal parser saw
five extra characters, and every stored key sorted after a bare comparison
target. Format identity and key structure are separate concerns, and the store
owns neither.

Why not a wrapper
-----------------

The first draft of this RFC put a four-byte envelope ahead of the payload
naming the codec. That was wrong and the reason is worth recording, because it
is the mistake the whole RFC exists to correct.

A wrapper makes the stored object readable only by code that knows about the
wrapper. ``jq`` fails, ``json.load`` fails, a text editor shows a few bytes of
noise before the document, and polars cannot open an Arrow file at all. It
would have reintroduced -- one layer down, and for every extension at once --
exactly the private format that motivated removing Fabric's codec.

Taking the codec from configuration instead costs a caller nothing it does not
already know -- it chose the format when it configured the store -- and buys
files every external tool already understands. Interoperability is the property
being paid for, and a self-describing byte stream only we can parse is not
interoperability.

Store contract
--------------

.. code-block:: cpp

    class ValueStore final
    {
      public:
        [[nodiscard]] ImmutableWriteResult
        write(std::string_view key, const ValueView &value,
              std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] Value read(std::string_view key, const ValueTypeMetaData *schema,
                                 std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] std::optional<Value>
        try_read(std::string_view key, const ValueTypeMetaData *schema,
                 std::optional<std::string_view> codec = {}) const;

        /** Value plus version token: a compare-and-swap loop needs both, and
            would otherwise reach past this store to the bytes. */
        [[nodiscard]] std::optional<StoredValue>
        try_read_versioned(std::string_view key, const ValueTypeMetaData *schema,
                           std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] CompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {}) const;
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
* **Transport framing.** RFC 0017 owns stream envelopes, which carry schema
  identity and sequence for a socket. This RFC persists objects, where the
  requirement is the opposite: no framing at all, so the file stays a file.
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
  call. It would let a store record what it wrote without touching the payload,
  and S3 carries content types natively. But it changes the ops table every
  backend implements, and the memory and local backends would have to store and
  return a field they otherwise have no use for -- for a benefit only a reader
  that mistrusts its own configuration would use. If a content type is added
  later for other reasons, a store may then verify against it; the format would
  still be selected here.
* **Codec as a template parameter on the store** — rejected: it moves the
  choice to compile time, which defeats a per-call override and forces every
  caller to name a format it should not care about.

No migration
------------

Fabric's durable format has no deployed readers: the extension is new and no
client store predates this change. The old codec is therefore **deleted rather
than retained**, and no dual-read, format sniffing, or version negotiation is
added.

That is worth stating explicitly because the alternative is expensive and
permanent. A compatibility path would have to be written, tested against
synthesised legacy objects, and carried until every store had been rewritten --
for objects that do not exist. Compatibility machinery added speculatively is
the hardest kind to remove later, because nothing proves it is unused.

Any store written by a previous build is discarded. Seven Fabric test files
exercise the current format; those asserting round-trip behaviour port to the
new store unchanged, and any asserting byte layout are deleted rather than
rewritten, since they test a codec that no longer exists.

``resolution_perf`` should confirm the metadata path is unaffected, rather than
the question being settled by argument. Metadata objects are small and one is
written per publication, against a frame write and a broker round trip.

Implementation plan
-------------------

Each step is independently reviewable and leaves the tree green.

1. ``ValueCodecOps``, the registry, and the ``"json"`` codec over
   ``JsonConverter``, with round-trip tests across the schema kinds the core
   codec supports.
2. ``ValueStore`` over ``ObjectStore``: default selection, per-call override,
   ``compare_exchange``, and keys passed through verbatim. Tests covering a
   mixed-codec store, an unknown codec name failing closed, and a local-backend
   object read back off disk as a json file.
3. Fabric writes and reads through ``ValueStore``; ``metadata_codec.{h,cpp}``
   deleted outright.
4. Generic key primitives moved beside ``require_valid_key()``; Fabric keeps its
   key layout.

Steps 1 and 2 are additive and land before any Fabric change, so the new surface
is exercised by its own tests before it carries a consumer.
