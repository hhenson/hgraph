RFC 0016: Object-Store Frame Persistence
========================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-10
:Target: Next hgraph minor release

Summary
-------

Give hgraph a general-purpose way to persist and retrieve Arrow frames against
object storage — S3 for production, a local filesystem for development, and
memory for unit tests — behind one configured store rather than three code
paths.

The mechanism is a *content store*: keyed, typed, and configurable in format
and location. Record/replay is its first consumer, not its definition. A second
consumer already exists in a downstream project that writes co-ordinated groups
of frames carrying frame-level metadata, and the contract below is stated so
that use is expressible without a private extension to it.

Nothing here requires a new third-party dependency. hgraph already links
``Arrow::arrow_shared``, and pyarrow's bundled ``libarrow`` exports the
filesystem layer this builds on.

Motivation
----------

``record_replay::FrameStoreOps`` already exists as the type-erased keyed store,
and its declaration anticipates this work:

   *"The default registration is an in-memory map; file / Arrow-dataset stores
   register over it."*

What is missing is any registration other than the default. A user who wants
recorded frames to outlive a process — or to be read by something that is not
this process — has to write the store themselves, and gets no help with format,
layout, or credentials.

Three needs are served by one mechanism:

**Environment parity.** The same graph should record to memory in a unit test,
to a directory in local development, and to a bucket in production, with the
environment selected by configuration rather than by a different call.

**Durable, interoperable output.** A recorded frame should be readable by
anything that reads Arrow — Spark, DuckDB, polars, pandas — without hgraph in
the loop. That constrains the format to Parquet or Arrow IPC, both of which are
already available through the linked Arrow build.

**Frames that travel with their meaning.** :doc:`rfc_0001_typed_frame_metadata`
puts frame-level identity, as-of time and provenance into the Arrow schema
metadata rather than into rows or a side object. Persistence must not lose it.

Ownership boundary
------------------

The store abstraction, its configuration, and the Arrow-backed backends are
**core**. Three things put them there rather than in an extension:

* the seam is already core (``FrameStoreOps`` in
  ``include/hgraph/types/record_replay.h``);
* the value type is already core (``Frame``, and ``Frame[Rows, Metadata]``
  under RFC 0001);
* no new dependency is acquired — ``arrow::fs`` is in the ``libarrow`` hgraph
  already links, so this adds no package, no toolchain, and no ABI surface
  beyond hgraph's own.

Per :doc:`../developer_guide/extension_policy`, a requirement originating in a
private downstream project is stated in generic terms before entering a core
RFC. The co-ordinated-frame requirement is therefore expressed below as
*grouped writes over a keyed store*, with no reference to the domain that
motivated it.

What stays out of core: bucket naming conventions, retention and lifecycle
policy, dataset partitioning strategy, and catalogue integration. Those are
application or extension concerns built *on* this contract.

C++ contract
------------

The existing ops table is retained unchanged as the runtime seam. Consumers
that only read and write by key are unaffected by this RFC.

.. code-block:: cpp

    struct FrameStoreOps
    {
        void *context{nullptr};
        void (*write)(void *context, std::string_view key, Frame frame){nullptr};
        Frame (*read)(void *context, std::string_view key){nullptr};
        bool (*contains)(void *context, std::string_view key){nullptr};
        void (*clear)(void *context){nullptr};
    };

Added: a described, configurable backend that produces such an ops table.

.. code-block:: cpp

    namespace hgraph::store
    {
        enum class Format { Parquet, ArrowIpc };

        /** Where frames live. Exactly one location is configured. */
        struct MemoryLocation {};
        struct LocalLocation  { std::string root; };
        struct S3Location
        {
            std::string bucket;
            std::string prefix{};
            /** Empty means resolve from the ambient AWS chain. */
            std::optional<std::string> region{};
            std::optional<std::string> endpoint_override{};
            std::optional<Credentials> credentials{};   // see below
        };

        using Location = std::variant<MemoryLocation, LocalLocation, S3Location>;

        struct FrameStoreConfig
        {
            Location    location{MemoryLocation{}};
            Format      format{Format::Parquet};
            Compression compression{Compression::Default};
            /** Reject a write whose key already exists. */
            bool        immutable{false};
        };

        /** Build a store; the caller registers it with set_frame_store. */
        [[nodiscard]] HGRAPH_EXPORT FrameStoreOps make_frame_store(FrameStoreConfig config);
    }

Credentials are explicit about the ambient case rather than silent about it:

.. code-block:: cpp

    struct Credentials
    {
        /** Resolve through the standard AWS chain: environment, profile,
            container, and instance metadata, in the SDK's order. */
        struct Ambient {};
        struct Explicit
        {
            std::string access_key_id;
            std::string secret_access_key;
            std::optional<std::string> session_token{};
        };
        /** A named profile from the shared credentials file. */
        struct Profile { std::string name; };
        /** Assume a role, refreshed by the SDK. */
        struct AssumeRole { std::string role_arn; std::optional<std::string> session_name; };

        std::variant<Ambient, Explicit, Profile, AssumeRole> source{Ambient{}};
    };

``Ambient`` is the default, so the common deployment configures nothing. An
application that must not read ambient credentials states another alternative
and gets a hard failure rather than an accidental fallback.

Python contract
---------------

The Python surface mirrors the configuration, not the ops table. The ops table
stays private; Python configures and registers.

.. code-block:: python

    from hgraph import frame_store, set_frame_store, FrameStoreConfig

    set_frame_store(FrameStoreConfig(location="memory"))                    # unit tests
    set_frame_store(FrameStoreConfig(location="file:///tmp/recordings"))    # development
    set_frame_store(FrameStoreConfig(                                       # production
        location="s3://bucket/prefix",
        format="parquet",
        region="eu-west-1",
    ))

A URL is the ergonomic form; the structured form is available for anything a
URL cannot say:

.. code-block:: python

    set_frame_store(FrameStoreConfig(
        location=S3Location(bucket="bucket", prefix="recordings",
                            endpoint_override="http://localhost:9000"),
        credentials=Profile("research"),
        format="ipc",
        immutable=True,
    ))

``record_replay_scope`` and the rest of the record/replay API are unchanged. A
graph does not know which store is active, which is the point.

Format
------

Both formats are supported and selected by configuration, because they answer
different questions:

.. list-table::
   :header-rows: 1
   :widths: 14 43 43

   * -
     - ``Parquet`` (default)
     - ``ArrowIpc``
   * - Reader reach
     - Anything that reads Parquet
     - Arrow-aware readers
   * - Size
     - Compressed, columnar
     - Larger; compression optional
   * - Write cost
     - Encode + compress
     - Close to a memcpy of the Arrow buffers
   * - Fidelity
     - Arrow schema restored via ``ARROW:schema``
     - Exact Arrow schema

Parquet is the default because durable records are read more often than
written, and usually by something other than hgraph. IPC is the right choice
for high-frequency intermediate output where the reader is also hgraph.

**Both preserve RFC 0001 metadata**, which is what makes frame-level identity
survive a round trip. Verified rather than assumed:

.. code-block:: text

    parquet preserves hgraph metadata: {b'hgraph.metadata.version': b'1',
        b'hgraph.metadata.schema': b'my.mod::AsOf',
        b'hgraph.metadata.field.as_of': b'2026-08-10T00:00:00'}
    IPC preserves hgraph metadata    : {same}

Parquet carries the Arrow schema — custom metadata included — in its key-value
metadata under ``ARROW:schema``; IPC carries the schema natively.

Multiple usages
---------------

The store is keyed and typed, so it is not specific to record/replay. Three
usages are in scope for the contract:

**Keyed frames** — the existing behaviour. ``write(key, frame)`` /
``read(key)``. Record/replay uses this and needs nothing more.

**Grouped writes.** A consumer that produces several frames which are only
meaningful together needs them to become visible together. This is the
generic form of the co-ordinated-frame requirement:

.. code-block:: cpp

    /** Frames written inside the group become readable atomically at commit.
        On destruction without commit, nothing is published. */
    class FrameStoreGroup
    {
      public:
        void write(std::string_view key, Frame frame);
        void commit();
    };

    [[nodiscard]] HGRAPH_EXPORT FrameStoreGroup begin_group(std::string_view group_key);

Object stores have no multi-object transaction, so "atomically" is delivered by
writing members under a temporary prefix and publishing a single manifest
object last. A reader that resolves through the manifest never observes a
partial group; a reader that addresses members directly can still do so.
Whether the manifest is *required* for group reads is an open question below.

**Frame-level metadata** is not a third mechanism. A grouped write of
``Frame[Rows, Metadata]`` values carries each member's metadata inside its own
Arrow schema, and the manifest records the group's membership and its own
metadata frame. No side-channel is introduced.

Runtime and lifecycle
---------------------

``arrow::fs::InitializeS3`` is process-global and must precede first S3 use,
with a matching finalize before process exit. The S3 backend initialises on
first construction and registers finalization once, so an application that
never configures S3 never initialises it. This is build-time/registration-time
work, not per-tick.

No part of this sits on the per-tick evaluation path: stores are consulted by
``record``/``replay`` nodes at their own cadence, and the ops table is looked up
once at node start, matching how the in-memory store is used today.

Failures are reported, not swallowed. A write that cannot reach the bucket
raises; it does not silently fall back to memory. Environment selection is a
configuration decision, and a store that quietly degrades would turn a
deployment error into corrupt-looking output much later.

Performance and memory
----------------------

Writes stream through ``arrow::fs::FileSystem::OpenOutputStream`` into the
Parquet or IPC writer, so a frame is not materialised a second time in memory.
Reads use ``OpenInputFile`` and Arrow's readers.

For S3 the dominant costs are request count and object size, not encoding —
which is why grouped writes publish one manifest rather than probing per member,
and why Parquet is the default. Multipart upload thresholds and read coalescing
are Arrow's defaults; exposing them is deferred until a workload needs it.

Compatibility and migration
---------------------------

Additive. The default registration remains the in-memory map, so a program that
configures nothing behaves exactly as it does today, including every existing
record/replay test. ``FrameStoreOps`` is unchanged, so a downstream store
already registered against it keeps working.

Nothing about the wire format of ``Frame`` changes. Persisted files are
ordinary Parquet or Arrow IPC and are not versioned by hgraph beyond the RFC
0001 metadata already inside them.

Alternatives considered
-----------------------

**``obstore`` / the Rust ``object_store`` crate.** The obvious library for this
job, and the one that prompted the RFC. Rejected for the core path: ``obstore``
is a PyO3 extension module, so a C++ consumer would have to embed CPython,
which contradicts the C++-first runtime rule. Using the underlying Rust crate
directly would require a C ABI shim and a Rust toolchain in the build — a
larger dependency decision than the capability warrants, given ``arrow::fs``
provides the same S3/filesystem/memory triple through a library already linked.
``obstore`` remains the right tool for Python-side tooling, where none of this
applies.

**AWS SDK for C++ directly.** S3 only, and would add the dependency that
``arrow::fs`` already absorbs.

**``arrow::fs::internal::MockFileSystem`` for the memory case.** Unnecessary,
and declared ``internal``. The existing in-memory store already serves unit
tests, so the test path acquires no new API.

**A dataset abstraction instead of a keyed store.** Arrow Datasets offer
partition discovery and predicate pushdown, which suits analytical reads but
not the keyed write/read the store contract is built on. A dataset view over
persisted output is a plausible later addition and does not conflict.

**Format fixed rather than configurable.** Rejected: interchange and
write-throughput are genuinely different objectives, and the user chooses which
matters. Fixing either would push the other into a bespoke store.

Unresolved questions
--------------------

* Must a grouped read resolve through the manifest, or may a reader address
  members directly and accept the risk of a partial view? The stricter rule is
  safer; the looser one is friendlier to external tools.
* Should ``immutable`` be the default? Recordings are usually write-once, and
  accidental overwrite is a real hazard — but replay-driven regeneration is a
  legitimate workflow that immutability makes awkward.
* Key-to-object-path mapping: how are ``/`` and other separators in a key
  treated? Transparent nesting is natural for humans browsing a bucket; strict
  escaping is unambiguous.
* Does the group manifest deserve a declared metadata schema under RFC 0001,
  rather than being an implementation detail of the store?
* Should compression be exposed per write, or only per store?

Acceptance criteria and test plan
---------------------------------

* The default store remains in-memory; the full existing record/replay suite
  passes unchanged with no configuration.
* Round trip through each backend — memory, local filesystem, S3 — for both
  formats, asserting frame equality including schema.
* RFC 0001 metadata survives every backend/format combination.
* A grouped write is not observable through the manifest before commit, and is
  complete after it; an abandoned group publishes nothing.
* Credential alternatives select as declared: ``Ambient`` resolves through the
  chain, ``Explicit``/``Profile``/``AssumeRole`` do not consult it, and a
  missing credential fails loudly.
* A failing store surfaces the error rather than degrading to memory.
* S3 coverage runs against a local S3-compatible endpoint via
  ``endpoint_override``, so it needs no cloud account. This is what makes the
  S3 path testable in CI rather than only in deployment.
* C++ tests for each backend, per AGENTS.md, with Python tests covering the
  configuration surface and bridge.

Implementation status
---------------------

Not started. This RFC records the design for review.

References
----------

* :doc:`rfc_0001_typed_frame_metadata` — frame-level metadata this must
  preserve.
* :doc:`../developer_guide/record_replay_table` — the ``FrameStoreOps``
  seam and the ``DATA_FRAME`` backend.
* :doc:`../developer_guide/extension_policy` — core versus extension ownership.
* Apache Arrow C++ ``arrow::fs`` filesystem layer, and the Parquet
  ``ARROW:schema`` key-value convention.
