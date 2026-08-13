RFC 0016: Object-Store Frame Persistence
========================================

:Status: Proposed; reference implementation in progress
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
consumer already exists in a downstream project that writes related frames
carrying frame-level metadata, and the contract below is stated so that use is
expressible without a private extension to it.

The filesystem layer this builds on is in the ``libarrow`` hgraph already
links. Arrow IPC is too. **Parquet is not** — it lives in a separate
``libparquet`` that no configuration currently links, and the Conan build
disables it outright. That is a real dependency task, not a free one, and it is
planned below rather than assumed away.

Scope
-----

This is **new native functionality on the 0.8 line**. Availability on the
Python-first ``release/0.5`` line is an explicit **non-goal**: the seam it
builds on (``store::FrameStore``) and the value type it moves
(``Frame``, and ``Frame[Rows, Metadata]`` under RFC 0001) are C++-runtime
constructs with no 0.5 counterpart, so there is nothing to keep at parity.

That does not remove the release/0.5 ``DataFrameStorage`` compatibility
surface. RFC 0019 adapts that Python API to complete-frame ``store``/``load``/
``has`` calls, while every supported production memory, file and S3 endpoint
uses this native contract.

The distinction matters because it is the opposite of the rule that applies to
*existing* behaviour. A capability both lines already claim to have must stay
at parity, and a divergence there is a bug to fix on 0.5. A capability only 0.8
has is simply a reason to be on 0.8. Nothing in this RFC should be read as
creating an obligation to back-port, and code written against it is 0.8-only by
construction.

Motivation
----------

``store::FrameStoreOps`` defines the passive type-erased keyed-store
operations. It preserves the seam originally introduced for this work:

   *"The default registration is an in-memory map; file / Arrow-dataset stores
   register over it."*

At proposal time, what was missing was any implementation other than the
default. A user who wanted
recorded frames to outlive a process — or to be read by something that is not
this process — had to write the store themselves, and got no help with format,
layout, or credentials.

Three needs are served by one mechanism:

**Environment parity.** The same graph should record to memory in a unit test,
to a directory in local development, and to a bucket in production, with the
environment selected by configuration rather than by a different call.

**Durable, interoperable output.** A recorded frame should be readable by
anything that reads Arrow — Spark, DuckDB, polars, pandas — without hgraph in
the loop. That constrains the format to Parquet or Arrow IPC. IPC is available
in the already-linked ``libarrow``; Parquet requires linking ``libparquet``,
which is what the dependency plan below is for.

**Frames that record how they were produced.**
:doc:`rfc_0001_typed_frame_metadata` puts frame-level values into the Arrow
schema metadata rather than into rows or a side object. The intended content is
not only identity — as-of, revision — but **provenance**: the query that
generated the frame. Dataset, date range, universe, filters, and whatever else
determined the result:

.. code-block:: text

    b'hgraph.metadata.field.dataset'  -> b'eod_prices'
    b'hgraph.metadata.field.start'    -> b'2026-01-01'
    b'hgraph.metadata.field.universe' -> b'["CL", "NG", "HO"]'
    b'hgraph.metadata.field.filters'  -> b'{"venue": "NYMEX"}'

A stored frame then answers *what produced this?* on its own, which is what
makes a persisted result auditable and reproducible rather than merely
retrievable. Persistence must not lose it, and a store that kept this
description anywhere but inside the object would break the property the moment
an outside tool read the file.

Ownership boundary
------------------

The store abstraction, its configuration, and the Arrow-backed backends are
**core**. Three things put them there rather than in an extension:

* the seam is already core (``FrameStoreOps`` and its owning handle in
  ``include/hgraph/types/frame_store.h``);
* the value type is already core (``Frame``, and ``Frame[Rows, Metadata]``
  under RFC 0001);
* no new *package* is acquired: ``arrow::fs`` and ``arrow::ipc`` are in the
  ``libarrow`` hgraph already links, and where Parquet is wanted the library
  ships in the same Arrow distribution rather than coming from a new supplier.
  No new toolchain, and no ABI surface beyond hgraph's own.

Per :doc:`../developer_guide/extension_policy`, a requirement originating in a
private downstream project is stated in generic terms before entering a core
RFC. The related-frames requirement is therefore expressed below in terms of
keys and frame metadata only, with no reference to the domain that motivated
it.

What stays out of core: bucket naming conventions, retention and lifecycle
policy, dataset partitioning strategy, and catalogue integration. Those are
application or extension concerns built *on* this contract.

C++ contract
------------

The public semantic contract is an owning, type-erased C++ ``FrameStore``. It
deliberately contains only storage concerns: complete-frame reads and writes,
existence, and explicit administrative clearing. Record/replay depends on this
contract, not on a concrete memory, filesystem, S3 or Python representation.

.. code-block:: cpp

    struct FrameStoreOps
    {
        void (*write)(void *, std::string_view, Frame,
                      std::optional<Compression>);
        Frame (*read)(void *, std::string_view);
        bool (*contains)(void *, std::string_view);
        void (*clear)(void *);
    };

    class FrameStore final
    {
        std::shared_ptr<void> context_;
        const FrameStoreOps *ops_;       // always non-null

      public:
        FrameStore(std::shared_ptr<void> context,
                   const FrameStoreOps &static_ops);
        void write(std::string_view key, Frame frame,
                   std::optional<Compression> compression = {}) const;
        [[nodiscard]] Frame read(std::string_view key) const;
        [[nodiscard]] bool contains(std::string_view key) const;
        void clear() const;
    };

``FrameStore`` is a value handle, not a strategy hierarchy. Its erased
``shared_ptr<void>`` makes ownership explicit: ``GlobalState`` copies into the
executor and back to the caller, and every copy refers to the same context.
Default and moved-from handles bind a canonical empty ops table, so dispatch
never branches around a null ops pointer. Empty-handle reads and existence
checks report absence; writes fail explicitly rather than silently losing
data. Concrete strategies and their containers are private to implementation
files.

**Registration is scoped to the graph run, not the process.** ``GlobalState``
is already owned by one graph instance and copied into and back from that run.
The process store in ``record_replay.cpp`` is only a fallback and cannot
represent a graph-selected destination. The precedent is
``set_config(GlobalStateView, ...)``; the selected store follows the same
graph-scoped ownership.

.. code-block:: cpp

    namespace hgraph::store
    {
        [[nodiscard]] HGRAPH_EXPORT FrameStore
        make_frame_store(FrameStoreConfig config);
    }

    namespace hgraph::record_replay
    {
        /** Register for the active run; the store is owned by GlobalState and
            released with it. Mirrors set_config's scoping. */
        HGRAPH_EXPORT void set_frame_store(
            GlobalStateView state, store::FrameStore store);
    }

The process fallback is another owning ``FrameStore`` handle, not a parallel
registration mechanism. Runtime nodes resolve the graph store first and then
dispatch through the fallback handle when no graph store was selected.

The configuration a backend is built from:

.. code-block:: cpp

    namespace hgraph::store
    {
        enum class Format { ArrowIpc, Parquet };

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
            Credentials credentials{};   // see below
        };

        using Location = std::variant<MemoryLocation, LocalLocation, S3Location>;

        struct FrameStoreConfig
        {
            Location    location{MemoryLocation{}};
            Format      format{Format::ArrowIpc};
            Compression compression{Compression::Default};
            /** Reject a write whose key already exists. Default: on. */
            bool        immutable{true};
        };

        /** Build a store; the caller registers it with set_frame_store. */
        [[nodiscard]] HGRAPH_EXPORT FrameStore
        make_frame_store(FrameStoreConfig config);
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

Python compatibility contract
-----------------------------

Python does not configure or implement the native memory, local-filesystem or
S3 endpoints. Those remain C++ stores with the contract above. Python is an
extension seam for compatibility bridges, most importantly the release/0.5
``DataFrameStorage`` API. The object installed for one graph implements only:

.. code-block:: python

    class PythonFrameStoreProtocol:
        def store(self, key: str, frame: Frame) -> None: ...
        def load(self, key: str) -> Frame | None: ...
        def has(self, key: str) -> bool: ...

No ``clear``, compression, immutability or segmentation operation crosses this
boundary. In particular, repeated-key behaviour belongs to the Python
implementation; native immutable-key policy is not imposed on a compatibility
adapter. ``MemoryDataFrameStorage`` continues to let release/0.5 code set and
retrieve complete tables directly.

Format
------

Both formats are supported and selected by configuration, because they answer
different questions:

.. list-table::
   :header-rows: 1
   :widths: 14 43 43

   * -
     - ``Parquet``
     - ``ArrowIpc`` (default)
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

Arrow IPC is the default because it is available wherever hgraph already links
Arrow. Parquet is selected explicitly when interoperability and storage size
justify the additional build capability.

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

The store is keyed and typed, so it is not specific to record/replay. Two
usages are in scope for the contract:

**Keyed frames** — the existing behaviour. ``write(key, frame)`` /
``read(key)``. Record/replay uses this and needs nothing more.

**Frames carrying their own definition.** A ``Frame[Rows, Metadata]`` value
persists with its metadata inside its own Arrow schema, so a stored frame is
self-describing: a reader recovers the row schema and the frame-level values
from the object itself. No side-channel, and no index object.

Keys are the only addressing mechanism. A consumer that writes several related
frames writes several keys, and relates them by naming them — a shared key
prefix, or values carried in each frame's own metadata. The store adds no group
or catalogue object.

Dependency plan
---------------

Raised in review, and the first draft was wrong to wave it away. What each
format needs, verified against the repository and the bundled Arrow build:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * -
     - Arrow IPC
     - Parquet
   * - Library
     - ``libarrow`` — already linked
     - ``libparquet`` — **linked by nothing today**
   * - Symbols
     - ``arrow::ipc::MakeFileWriter`` / ``RecordBatchFileReader::Open``
     - ``parquet::arrow::WriteTable`` / ``parquet::arrow::FileReader``
   * - pyarrow build
     - present
     - ships ``libparquet`` and ``include/parquet``
   * - Conan build
     - present
     - ``conanfile.py`` sets ``arrow.parquet = False``

``arrow::fs`` is likewise present in the pyarrow build — S3, local and the
filesystem headers are all there, confirmed by symbol inspection — but the
Conan configuration enables neither Parquet nor S3, so both need turning on
there.

Enabling Parquet therefore means, in order:

* flip ``arrow.parquet`` in ``conanfile.py``, and enable Arrow's S3 support in
  the same place, so the Conan configuration can build this at all;
* add an imported ``Parquet::parquet_shared`` target beside the existing
  ``Arrow::``/``ArrowCompute::``/``ArrowAcero::`` ones, discovered from the same
  pyarrow directory in the pyarrow configuration;
* link it, and export it through ``hgraph::options`` for installed consumers as
  the Arrow targets already are;
* stage ``parquet.dll`` beside the extension on Windows. The build tree already
  needs this for the Arrow DLLs — ``$<TARGET_RUNTIME_DLLS:_hgraph>`` covers a
  newly linked library automatically, but the wheel's ``install(FILES ...)``
  list is explicit and must gain the Parquet runtime;
* extend ``tools/audit_distribution.py`` and the installed-SDK consumer test so
  a wheel missing the Parquet runtime fails in CI rather than at first use.

The pyarrow build now discovers and stages Parquet when it is present. Conan
still disables it, so Parquet remains a build capability rather than a promise
of every installation; requesting it from a build without support fails
loudly.

Runtime and lifecycle
---------------------

``arrow::fs::InitializeS3`` is process-global and must precede first S3 use,
with a matching finalize before process exit. The S3 backend initialises on
first construction, so an application that never configures S3 never
initialises it. This is build-time/registration-time work, not per-tick.

Finalization, however, **cannot** be automated, and the prototype establishes
that by experiment rather than assumption. Both obvious mechanisms — a
``std::atexit`` handler and a function-local static whose destructor finalizes
— run *after* Arrow's own statics are gone, and the process terminates with
``mutex lock failed: Invalid argument``. Omitting finalization instead draws
Arrow's warning that a segfault at exit may follow.

The store therefore exposes ``store::finalize_s3()`` and states that S3
shutdown belongs to the native application, the only layer that knows when the
last S3 store is gone. It is safe when S3 was never initialised and safe to call
twice, so a caller need not track whether S3 was ever reached. The narrow
Python compatibility bridge cannot select S3 and therefore owns no S3
finalization lifecycle.

No part of this sits on the per-tick evaluation path: stores are consulted by
``record``/``replay`` nodes at their own cadence. The graph store is resolved
at replay start or record stop, never on the per-tick value path.

Failures are reported, not swallowed. A write that cannot reach the bucket
raises; it does not silently fall back to memory. Environment selection is a
configuration decision, and a store that quietly degrades would turn a
deployment error into corrupt-looking output much later.

Performance and memory
----------------------

Writes stream through ``arrow::fs::FileSystem::OpenOutputStream`` into the
Parquet or IPC writer, so a frame is not materialised a second time in memory.
Reads use ``OpenInputFile`` and Arrow's readers.

For S3 the dominant costs are request count and object size, not encoding,
which is the argument for selecting Parquet where objects are large and
long-lived. Multipart upload thresholds and read coalescing are Arrow's
defaults; exposing them is deferred until a workload needs it.

Testing
-------

Memory and local-filesystem backends are covered by ordinary unit tests, which
is most of the surface: the two filesystem backends differ only in which
``arrow::fs::FileSystem`` they hold, so key handling, immutability, both
formats, and metadata survival are exercised without any network.

The S3 path needs a real S3 protocol implementation, but *not* an AWS account.
Arrow's ``S3Options::endpoint_override`` points the same client at any
S3-compatible server, so the prototype's S3 test runs against a local MinIO in
Docker:

.. code-block:: sh

   docker run -d --name hgraph-minio -p 9010:9000 \
     -e MINIO_ROOT_USER=hgraphtest -e MINIO_ROOT_PASSWORD=hgraphtest123 \
     quay.io/minio/minio:latest server /data
   # create the bucket, then:
   export HGRAPH_S3_TEST_ENDPOINT=http://127.0.0.1:9010
   export HGRAPH_S3_TEST_BUCKET=hgraph-test
   export AWS_ACCESS_KEY_ID=hgraphtest AWS_SECRET_ACCESS_KEY=hgraphtest123
   ./hgraph_unit_tests "[s3]"

This exercises the genuine Arrow S3 filesystem — credentials, region, bucket
addressing, multipart write, immutability against an object store — with only
the endpoint differing from AWS.

The test is a hidden Catch2 case (``[.s3]``), so it does not run, and does not
fail, in a checkout without an endpoint; it is requested by tag. That is
preferred to reporting skipped, because ``catch_discover_tests`` surfaces a
skip as a CTest failure. CI can gain an S3 leg by running MinIO as a service
container and invoking the tag.

Compatibility and migration
---------------------------

Selecting the ``DATA_FRAME`` model creates a graph-owned native memory store
unless the graph already has another store. A custom C++ store supplies an
owned context plus a static ``FrameStoreOps`` table and installs the resulting
handle either on the graph or as the process fallback. A borrowed raw context
is deliberately no longer accepted: the graph copy-in/copy-out lifecycle must
not be able to outlive a store representation.

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

Decisions
---------

Recorded from review; each was an open question in the first draft.

**Immutable writes are the default.** A write to an existing key raises;
overwriting is opt-in per store. Recordings are write-once by nature and a
durable store makes accidental overwrite expensive — on S3 the previous object
is usually gone. The develop-and-re-record loop opts out explicitly, or clears
first, which is cheap in exactly the environments where that loop happens. This
differs from the current in-memory store, which overwrites silently; the change
is deliberate and applies to every backend so behaviour does not vary by
environment.

When metadata records the query that produced a frame, the key is often derived
from those same parameters, so that asking for a result and finding it are the
same operation. That pattern and immutability meet as follows: the caller tests
``contains(key)`` and either reads the existing frame or computes and writes
it. The second run of an identical query is then a read, not a rejected write.
A caller that writes unconditionally gets an error instead of silently
discarding the earlier result, which is the safer failure. The store needs no
API for this — key derivation is the caller's business — but the interaction is
recorded because immutable-by-default makes the ``contains``-first shape the
expected one rather than an optimisation.

**Keys map transparently to paths, with validation.** A key is the object-path
suffix and ``/`` nests, so a bucket browses as a tree and prefix listing works.
Write-time validation rejects ``..``, leading and trailing ``/``, empty
segments, and anything S3 cannot represent as an object key. Validation applies
in *every* backend, including memory, so a key that would fail against S3 fails
identically in a unit test rather than at deployment. This is the one place a
store deliberately rejects input the previous in-memory map accepted, and it is
documented rather than smoothed over.

**Compression is per store, overridable per write.** The store carries the
default; an individual write may override it. A large cold frame and a small
hot one have different economics, and the writer is the only party that knows
which it is holding.

**A frame's metadata schema is its definition.** Under RFC 0001 a stored
``Frame[Rows, Metadata]`` carries its frame-level values in its own Arrow
schema metadata. This RFC adds no index, no side object, and no second place
for a frame to be described: what a frame means travels inside the frame.

**Metadata encodes as byte keys and byte values, with JSON for nested values.**
This is the encoding Arrow's schema metadata provides — a map of binary to
binary — and RFC 0001 already uses it: one entry per populated field, atomic
values in their plain string form, and composite values through the
schema-directed JSON codec. Nothing new is required for a canonical mapping;
it is a metadata field like any other. Verified end to end, including the
persistence round trip this RFC is about:

.. code-block:: text

    b'hgraph.metadata.schema'             -> b'__main__::Provenance'
    b'hgraph.metadata.version'            -> b'1'
    b'hgraph.metadata.field.source'       -> b'EXCH'
    b'hgraph.metadata.field.revision'     -> b'7'
    b'hgraph.metadata.field.as_of'        -> b'2026-08-10T00:00:00Z'
    b'hgraph.metadata.field.canonical'    -> b'{"CL": "CRUDE", "NG": "NATGAS"}'

    parquet -> Provenance(source='EXCH', ..., canonical={'CL': 'CRUDE', 'NG': 'NATGAS'})
    ipc     -> Provenance(source='EXCH', ..., canonical={'CL': 'CRUDE', 'NG': 'NATGAS'})

The typed value is recovered after both Parquet and Arrow IPC, so a frame
keeps its definition through persistence without hgraph holding any state
about it.

Resolved format decision
------------------------

Arrow IPC is the default and Parquet is available when the build links
``libparquet``. This lets the core store use the already-required Arrow runtime
without making Parquet a mandatory dependency. Capability is explicit:
``store::parquet_available()`` reports it, and constructing an unsupported
Parquet store raises rather than silently changing format.

Acceptance criteria and test plan
---------------------------------

* The default ``DATA_FRAME`` location is a graph-owned in-memory store; the full
  existing record/replay suite passes unchanged with no destination
  configuration.
* Round trip through each backend — memory, local filesystem, S3 — for both
  formats, asserting frame equality including schema.
* RFC 0001 metadata survives every backend/format combination, and decodes
  back to the typed value — including a composite field carried as JSON.
* A write to an existing key is rejected under the default immutable setting,
  and succeeds when the store opts out.
* A key rejected by validation is rejected identically by every backend,
  including memory.
* A per-write compression override takes effect over the store default.
* Credential alternatives select as declared: ``Ambient`` resolves through the
  chain, ``Explicit``/``Profile``/``AssumeRole`` do not consult it, and a
  missing credential fails loudly.
* A failing store surfaces the error rather than degrading to memory.
* S3 coverage runs against a local S3-compatible endpoint via
  ``endpoint_override``, so it needs no cloud account. This is what makes the
  S3 path testable in CI rather than only in deployment.
* C++ tests for each backend, per AGENTS.md, with Python tests covering only
  the narrow compatibility bridge.
* Two runs configured with different destinations do not disturb each other's
  store, and a store released with its GlobalState leaves no live backend.
* The installed-SDK consumer test links and uses whichever format libraries the
  build claims to support.

Implementation status
---------------------

Native memory and local-filesystem stores, format handling, key validation,
immutable writes and graph-scoped ownership are implemented. S3 remains
build-option dependent. RFC 0019 consumes this graph-scoped store; segmented
recordings remain deferred to its step 7.

References
----------

* :doc:`rfc_0001_typed_frame_metadata` — frame-level metadata this must
  preserve.
* :doc:`../developer_guide/record_replay_table` — the graph-scoped frame-store
  seam and the ``DATA_FRAME`` backend.
* :doc:`../developer_guide/extension_policy` — core versus extension ownership.
* Apache Arrow C++ ``arrow::fs`` filesystem layer, and the Parquet
  ``ARROW:schema`` key-value convention.
