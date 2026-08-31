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

    struct ValueCodecBinding
    {
        std::shared_ptr<const void> owner;
        const void *handle;
    };

    struct ValueCodecOps
    {
        /** Resolve all schema-dependent state before evaluation. */
        ValueCodecBinding (*bind)(void *context,
                                  const ValueTypeMetaData *schema);

        /** Encode and decode through the bound schema handle. */
        void (*encode)(void *context, const void *bound,
                       const ValueView &value, ObjectBytes &out);
        Value (*decode)(void *context, const void *bound,
                        std::span<const std::byte> encoded);
    };

    void register_value_codec(std::string_view name, std::shared_ptr<void> context,
                              const ValueCodecOps &ops);

Registration is build-time machinery, not a per-tick path, so guarding the
registry with a counted ``TypeSystemMutex`` is sanctioned by the single-threaded
evaluation ruling rather than a departure from it. A store resolves its default
codec once, at construction, and keeps it: an ordinary call never reaches the
registry, and only an explicit per-call override looks one up by name.

**Binding: everything schema-dependent is resolved once.** ``ValueCodecOps``
has three entry points, not two. ``bind`` takes a schema and returns an opaque
owner/handle pair; ``encode`` and ``decode`` take that handle and must do no
schema-plan lookup and touch no type or realisation registry. The JSON codec
builds an owned ``BoundJsonConverter`` in ``bind``.  That plan captures the
active run's value
bindings, hierarchy alternatives, and immutable parser configuration, so its
read and write operations do not fall back to the interned-converter or graph
realisation registries.

A caller on the evaluation path binds while constructing **run-local
node/service state**, before evaluation, and carries the resulting
``BoundValueCodec``.  The binding retains the codec's erased
``shared_ptr<void>`` implementation context, its schema, and the explicit
erased owner/handle pair returned by ``bind``.  Retaining both owners is
required: a custom codec may return a context-owned handle, while the built-in
JSON codec returns an owned run-bound plan.  Encode and decode perform no
schema or realisation lookup, validate the schema in both directions, and
default or moved-from handles dispatch through a canonical throwing ops table
rather than a nullable one.

Reusable ``FabricConfig`` deliberately carries no bound handles.  It contains
one authoritative ``ObjectStore`` and the metadata codec name; a Fabric node
assembles a run-local ``ValueStore`` and private bounded revision/reference
handles in ``start``.  The concrete Fabric value-plan binding stays behind the
extension's ``impl`` boundary.  Keyed publisher and consistency state created
later during evaluation copy that run binding instead of resolving another
one.  Kafka nodes have no Fabric configuration, so they bind the transport codec in
``start`` and retain it in node ``State``.  The notifier remains an opaque
bounded transport and performs no semantic decode outside the graph.  The
unbound convenience calls on ``ValueCodec`` and ``ValueStore`` bind per call
and are for build-time and ad-hoc use only.

This is enforced rather than asserted: every type-system mutex is a counted
``TypeSystemMutex``.  Tests drive 64 complete Fabric metadata build, extract,
encode and decode cycles, including structural list materialisation, and
require ``type_system_lock_count()`` to be unchanged.  Separate regressions
cover a whole steady-state publication and creation of new keyed publisher and
consistency state after node start.  The core JSON binding participates in the
same contract: it owns a converter tree with pre-resolved ``ValueTypeRef``
bindings, parser configuration, and polymorphic discriminator alternatives.
The bound reader and writer therefore do not consult the type registry,
converter registry, or graph realisation snapshot during evaluation.

A binding is invalidated by a registry reset, which frees canonical metadata it
retains — exactly as it invalidates the bindings captured by ordinary nodes.
That is why binding belongs with one graph run and never in reusable
configuration or a process-lifetime static.  Ownership regressions explicitly
exercise binding teardown rather than relying on this lifetime rule as prose.

Selection
---------

The codec is **configuration**, at two levels:

* **Store default.** ``ValueStoreConfig::codec``, unset meaning ``"json"``.
* **Per call.** An optional codec argument on ``write``, ``read``,
  ``try_read``, ``try_read_versioned`` and ``compare_exchange``, which is what
  makes a mixed store possible -- small metadata as JSON beside a large record
  in a binary format -- without splitting the store.

Fabric exposes only the store-level choice as ``FabricConfig::metadata_codec``.
Its ``ObjectStore`` remains the sole metadata backend; the typed run binding is
derived from those two reusable configuration fields.  A separately supplied
``ValueStore`` would carry its own ``ObjectStore`` and permit revisions and
indexes to be written to a different backend from the one used for listings,
so that ambiguous configuration is intentionally not representable.

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

        [[nodiscard]] ValueCompareExchangeResult
        compare_exchange(std::string_view key, const ValueView &value,
                         std::optional<std::string_view> expected_version,
                         std::optional<std::string_view> codec = {}) const;

        [[nodiscard]] BoundValueStore
        bind_schema(const ValueTypeMetaData *schema,
                    std::optional<std::string_view> codec = {}) const;
    };

``BoundValueStore`` fixes the backend, codec and schema together for one run.
Its reads, writes and compare/exchange results are typed; even a losing
compare/exchange decodes the winning ``StoredValue`` through the same binding,
so graph code never drops to ``ObjectStore`` bytes.  The unbound convenience
form derives the binding from the candidate value's schema and returns the same
typed result.  Both forms forward the object store's version token unchanged.
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
3. Fabric writes and reads through private run-bound typed-store handles.  Their
   wrapper preserves Fabric's 16 MiB aggregate limit on every durable read,
   write and compare/exchange result without putting a domain policy into
   generic ``BoundValueStore``.  The former hand-written binary serializer is
   deleted; ``metadata_codec`` remains only as the declared-schema validation
   and transport-codec surface.
4. Generic key primitives moved beside ``require_valid_key()``; Fabric keeps its
   key layout.

Steps 1 and 2 are additive and land before any Fabric change, so the new surface
is exercised by its own tests before it carries a consumer.
