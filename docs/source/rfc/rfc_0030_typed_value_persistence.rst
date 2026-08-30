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

**Stored bytes are exactly the codec's output.** A ``.json`` object is a JSON
document a text editor opens and ``jq`` reads; an ``.arrow`` object is a file
polars loads directly. The store adds no header, framing, or trailer. The codec
is therefore named by the object key -- the conventional file extension -- which
every backend surfaces in listings and which a filesystem shows as an ordinary
file.

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

    void register_value_codec(std::string_view name, std::string_view extension,
                              std::shared_ptr<void> context, const ValueCodecOps &ops);

Registration is build-time machinery, not a per-tick path, so guarding the
registry with a counted ``TypeSystemMutex`` is sanctioned by the single-threaded
evaluation ruling rather than a departure from it. Codecs are resolved once when
a store is constructed or a call names one, never per value.

The extension is the conventional suffix for the format — ``json``, ``arrow``,
``parquet`` — because it is what names the codec in the stored key and what an
external tool recognises the file by.

The baseline codec is ``"json"``, extension ``json``, implemented over the
core's interned ``JsonConverter`` — ``to_json_string`` and ``from_json_string``.
It is required: a build without it is not a conforming persistence build.

Selection and self-description
------------------------------

Three levels, most explicit first:

* **The key.** ``records/alpha.json`` names its codec. A caller that cares
  states it, and the object is then self-describing to every reader, including
  ones outside this codebase.
* **Per call.** ``write(key, value, codec)`` selects for one object when the key
  does not, which is what makes a mixed store possible -- small metadata as
  JSON beside a large record in a binary format -- without splitting the store.
* **Store default.** ``ValueStoreConfig::codec``, unset meaning ``"json"``.

A write appends the codec's extension when the key does not already carry one,
so ``write("records/alpha", v)`` stores ``records/alpha.json``.
``resolve_key()`` exposes that name, because a caller handing the object to an
external reader needs it.

A read takes the logical key. When it names a codec, the object is fetched
directly. Otherwise the default's key is tried -- one ``get``, the common path
-- and failing that a single prefix listing finds an object written under
another codec, so a per-call override stays readable by a caller that does not
know one was used.

Extensions are unique across codecs: two codecs claiming ``json`` would make a
stored object ambiguous, and the registry rejects it.

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

Putting the codec in the key costs one listing on the uncommon read path and
buys files that every external tool already understands. Interoperability is
the property being paid for, and a self-describing byte stream that only we can
parse is not interoperability.

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
  call. It would make objects self-describing without a new envelope, and S3
  carries content types natively. But it changes the ops table every backend
  implements, and memory and local backends would have to store and return a
  field they otherwise have no use for. The envelope keeps the change inside
  one library. If a content type is added later for other reasons, the envelope
  becomes redundant and can be retired behind the same read path.
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
2. ``ValueStore`` over ``ObjectStore``: envelope, default selection, per-call
   override, ``compare_exchange``. Tests covering a mixed-codec store and an
   unknown codec name failing closed on read.
3. Fabric writes and reads through ``ValueStore``; ``metadata_codec.{h,cpp}``
   deleted outright.
4. Generic key primitives moved beside ``require_valid_key()``; Fabric keeps its
   key layout.

Steps 1 and 2 are additive and land before any Fabric change, so the new surface
is exercised by its own tests before it carries a consumer.
