RFC 0026: Versioned Dataflow Fabric
===================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-08-18
:Target: A new C++-first ``hgraph-fabric`` extension for durable,
         data-driven distributed computation

Summary
-------

Create ``hgraph-fabric``, a first-party extension for building distributed
computations whose independently running hgraph components communicate through
durable, atomic data rather than through direct calls.

A component subscribes to named data, computes whenever a usable input version
becomes available or its own schedule fires, and publishes another named data
item.  A publication is a complete ``Frame`` rather than a delta.  Each data
item has immutable versions, and each revision records the immediate input
versions which the publisher had accepted when it produced or retained its
output version.  Those records let a subscriber select a self-consistent view
even when upstream components run on different schedules, take very different
amounts of time, restart independently, or decide that an input change does not
change their result.

The production shape combines durable object storage and Kafka:

* ``hgraph-persistence`` stores every Frame version, every revision, an as-of
  index and a latest pointer; and
* Kafka carries keyed, conflatable revision notifications so a running graph
  can react promptly without polling the object store.

Persistence is authoritative.  Kafka is the live wake-up path.  A real-time
subscriber starts from the latest durable state and then follows Kafka.  A
simulation subscriber reconstructs state from the as-of index and replays
revision history at its original publication times.  A snapshot subscriber
loads one consistent view as of a requested time and ticks it once at graph
start.

This RFC fixes the semantic model and the public C++/Python shape implemented
by the first version of ``hgraph-fabric``.  The accepted implementation does
not add orchestration, a data catalogue, schema evolution, deletion, delta
distribution or event-accurate streaming facilities.

Motivation
----------

Recurring computation, not remote procedure calls
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Many useful computations are neither one-shot jobs nor continuously connected
request/reply services.  They are materialised results which are refreshed
whenever their inputs or schedule require it:

* a daily source table is rebuilt at the end of a business day;
* a normalisation step follows when that table is ready;
* several expensive derived tables run at different speeds;
* a live correction process can publish a fresher image between scheduled
  rebuilds; and
* downstream components should resume from the current state after a restart
  rather than demand that every upstream component run again.

Directly connecting those components makes their availability, latency and
lifecycle part of one distributed call graph.  A slow component holds a caller
open.  A restart requires service-level retry semantics.  Replaying the system
requires recreating the timing and availability of every service.  The data
which is the actual product of the computation becomes incidental to the
transport used to ask for it.

The fabric inverts that relationship.  The durable data item is the interface.
A producer and consumer agree on its identity and schema, but need not be alive
at the same time.  A producer may take seconds or hours.  Consumers observe the
result when it has been published and when their complete input view is
consistent.

Atomic state, not an event stream
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The unit of distribution is a complete immutable value.  In the first version
that value is an Arrow-backed ``Frame``.  The fabric does not describe row
inserts, updates and removals, and a subscriber does not have to apply an
unbroken delta sequence to reconstruct state.

This is deliberately a *latest-state* protocol:

* a slow real-time subscriber may conflate several notifications and load the
  newest usable version;
* Kafka need not retain every notification forever because the durable store
  contains the complete history;
* a component which requires every event should consume an event-accurate
  system such as Kafka directly; and
* a simulation which wants every historical state transition replays the
  durable as-of index rather than depending on the live notification topic.

Atomic does not mean small.  A Frame can contain a substantial dataset.  It
means that a data version is either wholly visible or not visible and that its
schema and object integrity are validated as one object.

Why versions are not enough
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each new output tick creates a data version.  That is sufficient to identify a
stored Frame, but not sufficient to say what a component has processed.

Consider component ``D2`` consuming ``D1``.  ``D1`` advances from version 2 to
version 3.  ``D2`` evaluates the new input and determines that its output is
unchanged.  Reusing output version 2 is correct, but downstream consumers still
need to know that ``D2`` version 2 is now compatible with ``D1`` version 3.

A revision records that acknowledgement:

.. code-block:: text

   D2 revision 2: dependencies {D1: 2}, output version 2
   D2 revision 3: dependencies {D1: 3}, output version 2

The data version changes only when the output ticks.  The revision changes
when the tuple of immediate input versions and output version changes.  A
component which sees new inputs but emits no new Frame therefore publishes a
small revision, not another copy of its data.

Why latest values are not necessarily consistent
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Suppose a graph consumes ``D1`` and ``D3``.  ``D3`` was computed from ``D2``,
and ``D2`` was computed from ``D1``.  The latest records are:

.. code-block:: text

   D1 revision 3: dependencies {},      output version 3
   D2 revision 3: dependencies {D1: 3}, output version 3
   D3 revision 2: dependencies {D2: 2}, output version 2

Loading ``D1`` version 3 beside ``D3`` version 2 would combine a new direct
input with a result whose ancestry still requires ``D1`` version 2.  The
subscriber must follow ``D3`` to the newest revision of ``D2`` carrying output
version 2, then follow that revision to ``D1`` version 2.  Its initial cut is
therefore ``D1`` version 2 and ``D3`` version 2 even though ``D1`` version 3 is
already stored.

When ``D3`` later acknowledges a ``D2`` revision compatible with ``D1`` version
3, the held ``D1`` version can be released.  If ``D3``'s own output version did
not change, only ``D1`` ticks into the graph.

Goals and non-goals
-------------------

The first fabric must:

* make named Frame versions durable and independently recoverable;
* record sufficient immediate lineage to find consistent input cuts;
* allow dependency sets to change between revisions;
* support scheduled and input-driven publishers;
* tolerate brief overlap between publishers for the same data id;
* conflate live updates without losing the ability to replay history;
* isolate unrelated consistency failures;
* support real-time, simulation replay and one-shot as-of reads;
* expose first-class C++ graph authoring and matching Python authoring; and
* add no work to ordinary compute-node evaluation paths which do not use the
  fabric.

It does not:

* place, schedule, restart, heartbeat or scale component processes;
* discover data by catalogue query, wildcard or prefix subscription;
* guarantee that arbitrary user code is deterministic or idempotent;
* describe non-fabric inputs such as timers, files or raw messages in lineage;
* deliver every live version to every subscriber;
* distribute row or time-series deltas;
* carry ``CompoundScalar`` or arbitrary scalar payloads in v1;
* provide an atomic transaction across several published data ids;
* evolve a data id's Arrow schema;
* delete, compact or retain only part of the published history; or
* support arbitrary feedback cycles between data ids.

Relationship to the historical data-fabric experiment
------------------------------------------------------

An old ``data_fabric`` branch explored dynamic, in-process construction of
ranked "atom" graphs.  A specialised scheduler would instantiate and connect
those graphs on demand inside one hgraph runtime.

This proposal reuses the word *fabric* but not that runtime design.
``hgraph-fabric`` connects independent graph executions through persisted data.
It does not create dynamic atoms, change graph rank, or introduce another
executor.  Ordinary hgraph graphs remain the unit of computation; the fabric
operators are boundary sources and sinks.

Terminology
-----------

``fabric prefix``
   A configured namespace and object-store prefix.  Together with a data id it
   isolates development, test and production fabrics which may use the same
   logical names.

``data id``
   The stable application-selected name of one atomic dataset within a fabric
   prefix.  ``(fabric prefix, data id)`` is its durable identity.

``data version``
   A positive, monotonically increasing, gap-tolerant integer local to a data
   id.  Each output tick allocates a new version and stores one complete Frame.

``revision``
   A positive contiguous counter local to a data id.  A revision records one
   accepted tuple of immediate fabric input versions and output version.

``dependency``
   An ordinary immediate fabric input identified by data id and data version.
   Ordinary dependencies constrain consistency.

``historical predecessor``
   The previous version of the same data id used to compute a new version.
   It is retained as provenance but is not a second visible value in the
   current consistency cut.

``as-of``
   The UTC system time at which a revision publication is committed.  It is an
   epoch-microsecond integer, strictly increasing within one data id.  It is
   knowledge time, not the business-effective time inside the Frame.

``head`` or ``latest``
   The newest accepted revision for a data id.  The mutable latest pointer is
   an index over immutable revision records, not the record itself.

``root input``
   A ``subscribe_data`` instance in one root graph execution.  Several root
   inputs may refer to related or unrelated data ids.

``consistency forest``
   A connected component of the ordinary dependency ancestry reachable from
   the root inputs.  The structure is generally a DAG because branches can
   join; *forest* emphasises that unrelated connected components advance
   independently.

``cut``
   One selected revision for each relevant frontier data id plus the resolved
   ancestry which proves those choices compatible.

``rolling ancestry``
   The rule that a required data version uses the newest currently available
   compatible revision carrying that output version.  A version-level
   dependency does not permanently pin a particular dependency revision.

``publisher``
   One ``publish_data`` sink.  A wired graph may contain several publishers for
   distinct data ids, but only one publisher instance for a particular data id.

``subscriber``
   One ``subscribe_data`` source.  The same subscription output may be used by
   several downstream publishers; each publisher independently records that
   data id when it is in its dependency set.

Architecture
------------

The production data path is:

.. code-block:: text

                  durable objects                      notification
              +-----------------------+             +----------------+
              | hgraph-persistence    |             | hgraph-kafka   |
              |                       |             |                |
   component  | data / revision /     |  full       | one topic per  |  component
   graph A ---+ as_of / latest        +--revision--->+ fabric, keyed  +--> graph B
              | memory / local / S3   |             | by data id     |
              +-----------+-----------+             +----------------+
                          ^                                  |
                          +--------- authoritative read -----+

The two paths have different jobs:

* object storage owns values, history and the accepted revision sequence;
* Kafka wakes a live subscriber and carries the complete accepted revision,
  populating its revision and dependency indexes without a manifest read in
  the common case;
* persistence remains authoritative for startup, reconnect reconciliation and
  targeted gap recovery; and
* startup and simulation are possible from persistence without a live
  producer.

An application graph remains ordinary hgraph composition:

.. code-block:: python

   @graph
   def normalised_prices() -> None:
       prices = subscribe_data("raw-prices", mode=SubscriptionMode.LIVE)
       instruments = subscribe_data(
           "instrument-reference", mode=SubscriptionMode.LIVE)
       result = normalise(prices, instruments)
       publish_data("normalised-prices", result)

The fabric wiring planner identifies the two subscription sources upstream of
``result``.  A revision of ``normalised-prices`` therefore records both current
input versions.  Whether ``normalise`` selected one conditional branch or
another at run time does not alter the declared dependency set: graph wiring is
the stable contract.

Ownership boundary
------------------

hgraph core owns
~~~~~~~~~~~~~~~~

Core continues to own only facilities which are generally useful to graph
execution and extension authoring:

* C++ and Python graph/operator authoring;
* time-series values, root sources and sinks;
* wiring-time graph inspection and traits, including the cold-path
  ``NodeBuilder::visit_child_graphs`` contract for compiled nested owners;
* graph-time and real-time scheduling;
* ``GlobalState`` lifetime and configuration scoping;
* extension operator registration; and
* the ``Frame`` value type and Arrow schema/value operations.

Core does not name fabric data ids, versions, revisions, stores, topics,
subscription modes or consistency policies.

The public extension seam has been validated from a separately built installed
SDK consumer.  A fabric-shaped proof can retain one wiring-owned plan, defer
and bind its source in an idempotent pre-rank finalizer, discover marked sources
through public upstream edges and through the exposed-output ancestry of
compiled child plans, and use a typed same-root subscription handle when the
data edge is deliberately hidden.  The nested case creates ``subscribe_data``
inside the child graph and feeds only the child output to an outer publisher;
unrelated child side effects are excluded from that output lineage.  It
therefore proves the ownership boundary rather than merely following an outer
input.
Fabric does not use the proof source to select runtime behavior.  Its public
operator requires an explicit subscription mode, and wiring selects the
concrete source implementation before execution.

The proof exposed one missing generally reusable core seam.  ``NodeBuilder``
now visits its immediate compiled child graph templates through a passive,
type-erased cold-path contract.  ``nested_``, ``map_``, ``mesh_``, associative
and ordered reduce, ``switch_`` and dynamic-TSL map all install the same
contract; ordinary nodes use its canonical no-op implementation.  Each view
also exposes the optional child output binding, allowing extensions to recurse
from the returned endpoint rather than scanning the complete child plan.  This
adds no evaluation-path work and carries no fabric ids, lineage or consistency
policy.

hgraph-persistence owns
~~~~~~~~~~~~~~~~~~~~~~~

Per :doc:`rfc_0025_hgraph_persistence`, durable storage policy belongs in the
separately installed persistence extension.  Fabric consumes, but does not
duplicate:

* the configured memory, local-filesystem and S3 locations;
* Arrow IPC/Parquet Frame persistence;
* credentials and S3 lifecycle;
* immutable object creation;
* object reads and prefix/range discovery; and
* compare/exchange of small named references such as ``latest``.

``hgraph-persistence`` now exposes the reusable public ``ObjectStore`` contract
anticipated by :doc:`rfc_0023_graph_checkpoint_recovery` and RFC 0025's durable
checkpoint work.  Its memory, local-filesystem and S3 strategies provide typed
immutable creation, typed reads, paginated ordered listing and conditional
reference publication; ``FrameStore`` uses the same atomic backend semantics.
That contract remains persistence-owned.  Fabric must consume it rather than
grow a private second S3 client or put persistence types back into core.

hgraph-kafka owns
~~~~~~~~~~~~~~~~~

The Kafka extension owns broker configuration, librdkafka resources, producer
acknowledgements, consumer positions, topic interaction and worker lifecycle as
specified by :doc:`rfc_0015_kafka_extension_api`.

Fabric supplies the revision-notification schema and its state-distribution
policy.  It composes the Kafka services rather than exposing librdkafka types.
No generic messaging abstraction is promoted to core by this RFC.

hgraph-fabric owns
~~~~~~~~~~~~~~~~~~

The new extension owns:

* data-id, version, revision, dependency and subscription-mode types;
* the ``publish_data`` and ``subscribe_data`` operator contracts;
* wiring-time dependency discovery;
* durable fabric key layout and metadata schemas;
* publication ordering and first-writer-wins reconciliation;
* consistency-forest construction and cut resolution;
* real-time conflation, simulation replay and snapshot semantics;
* fabric-specific store/notifier composition and configuration;
* diagnostics and tests; and
* Python registrations of the same C++ contracts.

Packaging and activation
------------------------

The implementation follows the existing first-party extension layout:

* repository directory ``extensions/fabric``;
* CMake package and distribution ``hgraph-fabric``;
* exported target ``hgraph::fabric``;
* root build option ``HGRAPH_BUILD_FABRIC_EXTENSION``;
* Python package ``hgraph_fabric`` with an abi3 native module;
* optional installed target ``hgraph::fabric_kafka`` for the production
  transport adapter; and
* C++ namespace ``hgraph::fabric``.

The extension depends on compatible versions of hgraph core and
``hgraph-persistence``.  The production notifier additionally depends on
``hgraph-kafka``; the memory notifier used by unit tests must not require a
broker.  Importing core does not import any of these extensions.

As with the other extensions, native registration is idempotent and uses the
registration-installer mechanism so registry rebuilds reapply its operator and
value registrations.  Selecting a fabric backend or explicitly importing
``hgraph_fabric`` is the Python load point.

Checkpoint-0 dependency baseline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The installed public dependency baseline is the 0.8 API family, proven at
0.8.0.  A native fabric consumer uses these CMake packages and targets:

.. list-table::
   :header-rows: 1
   :widths: 32 32 36

   * - Package
     - Imported target
     - Fabric use
   * - ``hgraph``
     - ``hgraph::core``
     - graph/operator authoring, ``Frame`` and runtime services
   * - ``hgraph-persistence``
     - ``hgraph::persistence``
     - immutable objects, ordered listing, conditional references and Frames
   * - ``hgraph-kafka``
     - ``hgraph::kafka``
     - keyed notification publish, delivery reports and subscriptions

``tests/fabric_dependency_consumer`` is configured only against an installed
SDK and links the two extension targets, which carry the core target
transitively.  It proves one native process can create/read/list immutable
metadata, conditionally advance a head, round-trip a Frame, and send and
receive a keyed Kafka notification after observing its correlated broker
delivery report.

The probe also fixed the fabric-owned Kafka profile without changing the
general Kafka extension's valid policies.  Fabric requires idempotent
production with ``acks=all`` (or ``-1``), ``Fail`` rather than ``Drop`` at
consumer and final producer queue boundaries, and an unfiltered independent
subscription to every partition of its configured topic.  Startup uses
``Committed`` with ``Earliest`` fallback and explicit commits after durable
validation.  ``Recovering`` and ``Live`` provide the public lifecycle boundary
needed to load and reconcile the durable image; accepted records received
before that handoff completes are retained and conflated by fabric in
checkpoint 6.

The partition count of a fabric topic is a deployment invariant.  Expanding
an existing topic would both remap keyed data ids and leave an independently
assigned running subscriber unaware of the new partition until it restarts.
An installation which needs a different partition count creates a new fabric
topic/namespace and migrates deliberately.

The installed probe uses librdkafka's in-process protocol cluster.  Checkpoint
6 subsequently added the actual-broker restart, rebalance, delivery-failure,
partition-ordering and bounded-backpressure gate.

Public value contract
---------------------

The following registered shapes are normative.  C++ and Python represent the
same scalar schemas; the ``Input`` structures are C++ convenience values used
to build and decode those schemas, not a second contract.

.. code-block:: cpp

   namespace hgraph::fabric
   {
       using DataVersion = Int;
       using RevisionId  = Int;

       using DataDependency =
           Bundle<"hgraph.fabric::DataDependency",
                  Field<"data_id", Str>, Field<"version", Int>>;

       using DataRevision =
           Bundle<"hgraph.fabric::DataRevision",
                  Field<"format_version", Int>, Field<"data_id", Str>,
                  Field<"revision", Int>, Field<"output_version", Int>,
                  Field<"dependencies", HomogeneousTuple<DataDependency>>,
                  Field<"self_predecessor", Int>, Field<"as_of", DateTime>>;

       enum class SubscriptionMode
       {
           Live,
           Replay,
           Snapshot,
       };
   }

``dependencies`` is sorted by the canonical UTF-8 data id and contains no
duplicate id.  It is the complete immediate fabric dependency set for that
publication.  It does not contain the output data id; an optional prior-self
edge uses ``self_predecessor`` instead.

The revision does not duplicate the Frame's Arrow schema or persistence-owned
integrity metadata.  The first accepted Frame fixes the data id's schema, and
subsequent Frames are validated against it through the persistence value
contract.  An output tick is still the publisher's explicit statement that it
produced a new atomic result, even when the value is equal to the prior Frame.

``self_predecessor`` is optional provenance for the specific case where a
publisher constructed the output from a prior version of the same data id.  It
is not populated merely because one version follows another, and it is not
required for recovery, replay or consistency resolution.

The revision intentionally contains no publisher or process identity.  The
data id and accepted contents are the publication provenance.  Operational
logging may include process details, but they do not become durable semantics.

Operator contract
-----------------

C++ authoring
~~~~~~~~~~~~~

The C++ surface consists of ordinary operator markers and wiring helpers.  Its
intended shape is:

.. code-block:: cpp

   namespace hgraph::fabric
   {
       Port<TS<Frame>> subscribe_data(
           Wiring &w,
           Str data_id,
           SubscriptionMode mode,
           std::optional<DateTime> as_of = {});

       void publish_data(
           Wiring &w,
           Str data_id,
           Port<TS<Frame>> value,
           DependencySelection dependencies = DependencySelection::automatic());
   }

``data_id``, ``mode``, ``as_of`` and an explicit dependency selection are
wiring-time scalars.  The required ``Live``, ``Replay`` or ``Snapshot`` mode
selects its concrete source overload during wiring.  ``Live`` uses the
real-time Kafka service push-source edge.  ``Replay`` and ``Snapshot`` use
ordinary deterministic scheduled sources and never construct a push source.
No source inspects executor mode or branches on subscription mode in ``eval``.

A time-varying data id would change the durable identity and is therefore not
accepted by this operator.  Applications requiring a dynamic family wire keyed
component graphs explicitly rather than branch on a string in the publisher's
evaluation path.

Python authoring
~~~~~~~~~~~~~~~~

Python adapts the same operators:

.. code-block:: python

   def subscribe_data(
       data_id: str,
       *,
       mode: FabricSubscriptionMode,
       as_of: datetime | None = None,
   ) -> TS[Frame]: ...

   def publish_data(
       data_id: str,
       value: TS[Frame],
       *,
       dependencies: DependencySelection = AUTO,
   ) -> None: ...

``as_of`` is required in ``SNAPSHOT`` mode and rejected in ``LIVE`` and
``REPLAY`` modes.  Callers must choose ``SNAPSHOT`` for a one-shot value,
``REPLAY`` for deterministic history, or ``LIVE`` for Kafka-driven updates.

Configuration
~~~~~~~~~~~~~

Resource selection belongs to ``GlobalState`` and is copied into and back out
of execution with the normal graph state.  The semantic configuration includes:

.. code-block:: cpp

   struct FabricConfig
   {
       Str                              prefix;
       persistence::store::ObjectStore objects;
       persistence::store::FrameStore  frames;
       Notifier                         notifications;
       std::size_t                      notification_candidate_limit;
   };

The concrete persistence spelling is resolved with the prerequisite store
contract described above.  ``prefix`` and every data id use the common
persistence key validation rules.  A configuration error fails at wiring or
start; it never falls back from S3/Kafka to process memory.

Dependency discovery
--------------------

Automatic discovery happens once, after graph wiring and before execution.
For each ``publish_data`` instance the planner walks its upstream wiring and
collects every reachable ``subscribe_data`` source.  It records the union of
statically possible routes.  It does not inspect which branch happened to be
selected during an evaluation.

This has three consequences:

* lineage work does not run in the ordinary node hot path;
* a subscription which is part of a conditional computation remains a
  dependency even on ticks where that branch did not contribute; and
* two publishers sharing one subscription independently include its current
  version in their own revisions.

An explicit dependency selection is available for a graph abstraction whose
relevant sources are hidden from normal traversal or for a deliberate
conservative dependency declaration.  Explicit handles must name
``subscribe_data`` results in the same wired root graph.  They do not accept
arbitrary strings which could claim lineage the graph did not load.

All fabric subscriptions in one root execution register with a root ingress
coordinator.  That coordinator does not impose one global barrier.  It builds
the ancestry relation, partitions it into independent forests, and releases
each forest when that forest has a consistent cut.  It also watches revision
notifications for every data id in the reachable transitive ancestry, not only
the root subscription ids.  A same-output-version revision of an intermediate
dependency can make a held forest consistent without ticking that intermediate
Frame into the user graph.

Durable representation
----------------------

Identity and fixed schema
~~~~~~~~~~~~~~~~~~~~~~~~~

The durable identity is the pair ``(fabric prefix, data id)``.  A name need
only be unique within that namespace, allowing the same logical ids in local,
test and production stores.

The first accepted data version fixes the canonical Arrow schema.  Every later
publication validates exact schema identity before its revision can win.
Business schema evolution uses a new data id in v1.  This avoids silently
feeding a graph a Frame it was not wired to consume and leaves compatible
schema evolution to a later, explicit RFC.

Data versions
~~~~~~~~~~~~~

Every output tick allocates:

.. code-block:: text

   version = max(system_clock_epoch_milliseconds, previous_version + 1)

The number is locally monotonic and need not be continuous.  It is useful in
paths and diagnostics but is not a globally ordered timestamp.  Versions from
different data ids are compared for equality only where a dependency requires
one; their numeric magnitudes do not establish causality.

The Frame is written to:

.. code-block:: text

   <fabric-prefix>/<encoded-data-id>/data/<fixed-width-version>

The encoding is canonical, reversible and accepted by the persistence key
validator.  Numeric path segments are fixed-width unsigned decimal so lexical
ordering agrees with numeric ordering.  A complete encoded object key is
limited to 1,024 bytes across every backend, matching S3's portable key limit;
the configured prefix and encoded data id must leave room for the category and
fixed-width ordinal.  The logical 4,096-byte metadata limit therefore does not
promise that every data id fits every durable namespace.  The physical Frame
format is selected by ``hgraph-persistence`` configuration.

Revisions
~~~~~~~~~

The first revision is 1.  Every accepted revision is exactly one greater than
its predecessor.  A new revision is required when either:

* one or more immediate input versions changed; or
* the output version changed because the publisher received an output tick.

The predecessor is always ``revision - 1`` and is not stored in the revision
record.  Unlike data versions, revision ids have no gaps.  A missing preceding
slot is therefore a malformed history rather than an alternate lineage which
needs an explicit parent reference.

No revision is written for an identical tuple.  The full immutable revision is
stored at:

.. code-block:: text

   <fabric-prefix>/<encoded-data-id>/revision/<fixed-width-revision>

Dependencies are version references rather than revision references.  Several
revisions of a dependency may legitimately carry the same output version while
acknowledging newer inputs.  Resolution therefore searches those revisions
newest-first and uses the newest compatible one.  This rolling interpretation
is intentional: a revision describes compatibility with versioned state, not
an immutable globally timed proof of every transitive revision selected at the
original publication instant.

Optional historical self-predecessor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A live updater may construct ``A`` version 8 from ``A`` version 7.  Recording
``A:7`` as an ordinary dependency of ``A:8`` would require two current versions
of the same id and appear cyclic.  ``self_predecessor=7`` instead identifies a
temporal state chain.

The field preserves the temporal derivation chain for audit and provenance.
The consistency resolver ignores it: it does not add ``A:7`` to the current
frontier or use the predecessor's ancestry to constrain a cut containing
``A:8``.  General feedback such as ``A -> B -> A`` is not covered by this
provenance field and is rejected.

As-of index and latest pointer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every accepted revision receives:

.. code-block:: text

   as_of = max(system_clock_epoch_microseconds, previous_as_of + 1)

This is the publisher host's ordinary UTC system clock at publication.  The
monotonic bump only prevents duplicate or decreasing keys for one data id.  No
clock service or total fabric order is implied.  Dependency resolution, not
clock comparison, establishes consistency across ids.

The immutable index entry:

.. code-block:: text

   <fabric-prefix>/<encoded-data-id>/as_of/<fixed-width-epoch-microseconds>

contains the accepted revision id.  A query at ``T`` chooses the greatest
index time less than or equal to ``T``.  ``latest`` is a small conditionally
updated reference to the newest revision.  Revision slots remain the durable
source of truth when a crash leaves one of these derived indexes behind.

Retention
~~~~~~~~~

V1 never deletes a published data version, revision or as-of index entry.
This is stronger than reachability retention: the complete accepted history is
available for simulation and audit.  A failed competing writer may leave an
unreferenced candidate data object; it is not a published version because no
accepted revision names it, and V1 does not attempt to collect it.

Any future retention or compaction policy must preserve every object needed by
retained revision ancestry and will require a separate RFC.  Object-store
lifecycle rules must not delete fabric prefixes behind the protocol's back.

Revision serialisation
~~~~~~~~~~~~~~~~~~~~~~

Revision, as-of and latest objects use the fabric-owned ``HGFM`` binary
envelope and are stored through ``hgraph-persistence``.  Version 1 starts with
this eight-byte header:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Bytes
     - Type
     - Meaning
   * - 0--3
     - four octets
     - ASCII magic ``HGFM``
   * - 4
     - unsigned byte
     - envelope version, exactly ``1``
   * - 5
     - unsigned byte
     - object kind: revision ``1``, as-of reference ``2`` or latest reference
       ``3``
   * - 6--7
     - unsigned 16-bit
     - big-endian flags

A revision permits only flag bit 0, which states that
``self_predecessor`` is present.  Its payload is, in order:

.. code-block:: text

   revision             uint64 big-endian
   output_version       uint64 big-endian
   as_of                 int64 epoch microseconds, big-endian two's complement
   data_id               uint32 byte length + UTF-8 bytes
   dependency_count      uint32 big-endian
   dependencies          repeated (uint32 id length + UTF-8 id + uint64 version)
   self_predecessor      uint64, only when flag bit 0 is set

Revision, output and dependency versions are positive and fit the public
signed 64-bit contract.  Dependencies are strictly increasing by their UTF-8
data-id byte sequence, contain no duplicate or self id, and are limited to
65,535 entries.  A data id is non-empty valid UTF-8, contains no Unicode
control code point and occupies at most 4,096 bytes.  The complete object is
limited to 16 MiB.

As-of and latest references set flags to zero and contain one positive
big-endian ``uint64`` revision id after the header.  Their media types are,
respectively:

* ``application/vnd.hgraph.fabric.revision.v1+binary``;
* ``application/vnd.hgraph.fabric.as-of.v1+binary``; and
* ``application/vnd.hgraph.fabric.latest.v1+binary``.

Unknown versions, kinds or flags, non-canonical field order, malformed UTF-8,
bounds violations and trailing bytes fail closed.  The fixtures under
``extensions/fabric/tests/fixtures`` are consumed by both the C++ and Python
tests, fixing one language-independent byte representation.

The Frame object and persistence format carry the Arrow schema and any
integrity metadata.  A reader validates the revision encoding, path id and
revision id, then loads ``output_version`` through the persistence value
contract.  Schema and payload integrity are therefore checked at the Frame
boundary rather than duplicated in revision metadata.

Publication protocol
--------------------

Ordinary publication
~~~~~~~~~~~~~~~~~~~~

``publish_data`` observes two independent triggers:

* the visible Frame input, whose tick creates a new data version; and
* a hidden cut input projected from the publisher's fabric dependencies, whose
  version-tuple change can create a revision without a Frame tick.

The publication sequence for a candidate revision ``R`` is:

1. Read and validate the accepted head, repairing a contiguous completed
   revision which is ahead of ``latest`` when necessary.
2. Capture the current versions of the publisher's complete dependency set.
3. If the Frame ticked, allocate a new data version and conditionally write the
   Frame object.  If it did not tick, retain the previous output version.
4. If the resulting dependency/output tuple equals the accepted head tuple,
   stop; there is no new revision.
5. Allocate revision ``R = latest.revision + 1`` and a monotonic as-of, and
   construct the complete revision record.
6. Conditionally create the immutable ``revision/R`` object.  The first write
   to succeed is the winner.
7. The loser discards this publication attempt, reloads the accepted winner and
   does not advertise its losing candidate or turn it into revision ``R+1``.
8. The winner writes the immutable as-of index entry and conditionally advances
   ``latest``.
9. Publish the complete accepted revision to the fabric Kafka topic and observe
   broker acknowledgement.  A failed delivery is retried without changing the
   already accepted revision.

The revision-slot race precedes Kafka delivery, so Kafka contains accepted
revisions rather than losing candidates.  A subscriber can validate and index
the complete message in memory without reading its immutable slot on every
tick.  A non-contiguous message reveals a gap and triggers targeted durable
recovery for the missing history.  Startup and reconnect likewise reconcile
the durable accepted head before entering the live state.

The immutable revision slot is the publication commit point.  ``latest`` and
the as-of index make discovery efficient but can be repaired from a completed
slot after a crash.  All objects referenced by a revision are durable before
that slot is created.  Kafka acknowledgement is not part of acceptance and a
delivery failure cannot roll back the durable winner.  A publisher service
which restarts or reconnects reconciles and re-advertises an accepted head
whose delivery may have been interrupted; duplicate accepted notices are
harmless.

First publication and unchanged outputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A data id has no valid revision until its publisher has produced its first
Frame.  An input-cut change before that first output cannot create a revision
which refers to a nonexistent output version.

After the first output, an input change schedules the publisher even if its
ordinary calculation emits nothing.  The publisher records the new input
tuple against the existing output version.  This is the acknowledgement which
allows downstream forests to become consistent without copying the Frame.

An output tick without a fabric-input change, for example from a schedule,
creates a new data version and revision using the current input tuple.  An
output tick always means a new version even when its value equals the prior
Frame.

Competing publishers
~~~~~~~~~~~~~~~~~~~~~

One wired graph has at most one publisher for a data id, but separate process
instances may overlap.  Expected examples are deployment failover and a
scheduled initialiser briefly overlapping a live updater.  Arbitrary
independent active writers are not the normal operating model.

Both writers may prepare Frame objects and send candidate notices.  Only one
can create the next immutable revision slot.  The winner becomes the accepted
history.  The loser drops its attempt unconditionally; it does not compare
component identity, attempt to merge Frames or replay itself as the following
revision.  A future input or scheduled output tick may naturally create a
later candidate from the newly observed head.

Two writers can select the same millisecond version.  The Frame write is also
conditional.  The persistence layer may recognise an existing object as an
idempotent retry of the same immutable Frame; any conflicting object at that
path loses the attempt before revision publication.  This comparison does not
add schema or integrity fields to ``DataRevision``.  Writers selecting different
candidate versions can leave an unreferenced loser object, which is harmless
because discovery follows accepted revisions rather than listing the ``data``
prefix.

Crash boundaries
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 31 29 40

   * - Crash point
     - Durable consequence
     - Recovery
   * - Before Frame write completes
     - No visible publication
     - Retry on a later publisher evaluation.
   * - After Frame write, before revision creation
     - An unreferenced candidate Frame may remain
     - It is ignored because no revision names it.
   * - After revision creation, before as-of/latest
     - The accepted immutable revision is ahead of its indexes
     - Publisher or subscriber startup repairs the contiguous indexes before
       advertising or consuming the recovered head.
   * - After as-of, before latest
     - Historical lookup sees the revision while latest lags
     - Startup repair advances latest conditionally.
   * - After latest, before Kafka acknowledgement
     - The publication is accepted but its live notice may be absent
     - Delivery retries advertise the same accepted revision; startup and
       reconnect reconciliation can re-advertise the durable head.
   * - After Kafka acknowledgement, before local completion
     - Publication and notification are complete
     - Retrying observes the identical accepted tuple and performs no write;
       duplicate accepted notices are harmless.

There is no claim of one transaction spanning Kafka and S3.  The ordering
above makes every intermediate state distinguishable and repairable.  Durable
acceptance intentionally precedes the best-effort live wake-up.

Notification contract
---------------------

Each fabric namespace uses one configured Kafka topic.  The record key is the
canonical data id and the value is the complete accepted ``DataRevision``.
Partitioning by key preserves per-data-id order.  No ordering across data ids
is required.

The delivery handshake is ordinary graph composition.  Once persistence has
accepted a revision, the Fabric publication node exposes it in a bounded keyed
time series.  A notification-flow node selects one candidate, exposes a
reference to that same ``Shared<DataRevision>`` endpoint on an ordered request
edge, and receives the correlated delivery report on another edge.  Its active
candidate, retry count, completion feedback and diagnostic counters are
time-series state.  A retriable report temporarily unbinds and then rebinds the
same reference on the next graph cycle, so retry neither copies the revision
nor creates a new shared allocation.  Completion feedback removes the
candidate and advances the publication state machine.

``FabricConfig::notification_candidate_limit`` is the explicit resource bound
on durable candidates awaiting the graph-native transport (1024 by default).
Saturation is fatal rather than implicit backpressure: publication requests
have already entered the graph and candidates may already be durable, so merely
stopping candidate extraction would transfer an unbounded broker stall into
the per-data-id publication queues without applying admission at the sender.
Hosts size the bound for the maximum notification lag they are prepared to
retain.

The Kafka service task is deliberately narrower: a publish sink submits the
request to the broker and the service's FIFO root push source returns delivery
reports and other broker events in admission order.  It does not own Fabric
candidate selection, correlation, retry or completion state.  This is also the
only off-graph boundary in the notification path.

The topic and the in-process handoff are suitable for compaction/conflation:

* a subscriber needs the newest accepted revision, not every notice;
* a notice carries a complete accepted revision and populates the in-memory
  revision, output-version and dependency indexes;
* skipping an intermediate revision cannot make the latest Frame
  unreconstructable; and
* complete historical replay comes from the as-of index.

After real-time startup succeeds, Kafka delivery is the wake-up mechanism; the
subscriber does not periodically poll every latest key.  Startup subscribes
before completing the durable image handoff, then drains notices which raced
with that read.  On reconnect it repeats the same reconciliation.  A
deployment must configure acknowledged production and retention consistently
with that contract.  The topic consumer observes all partitions required by
the fabric binding.  The coordinator discards unrelated ids after key parsing
and retains notices for root ids and the dynamically discovered transitive
closure of their forests.

A valid relevant message whose revision is not contiguous with the cached head is held
while the missing range is recovered from immutable storage with bounded
backoff.  This is a targeted continuation of a Kafka wake-up, not a store-wide
polling loop.  The live-session cache remains conflated and bounded to one
newest accepted revision per observed data id; unrelated topic traffic is not
retained.  Only contiguous accepted history participates in cut resolution.

Subscription and consistency
----------------------------

Observed revisions
~~~~~~~~~~~~~~~~~~

The ingress coordinator maintains, for each directly subscribed or
transitively watched data id:

* the greatest observed accepted revision;
* cached revision records needed by current searches;
* the currently exposed data version, if any;
* a pending newer candidate; and
* the consistency forest to which the root belongs.

Notices are conflated by data id before graph-thread work is scheduled.  The
graph thread resolves from immutable metadata and emits Frames only after the
cut is ready.  Broker callbacks and object-store workers never mutate a
time-series output directly.  A revision of a transitive id invalidates and
re-resolves every forest whose current or pending ancestry contains that id,
even when no root output version changed in the notice itself.

Forests rather than one global barrier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two root inputs with no shared ordinary ancestry do not wait for each other.
Their ancestry DAGs form separate forests, and each forest advances as soon as
its own newest consistent cut is available.

For example:

.. code-block:: text

   Forest A                         Forest B

   D3 -> D2 -> D1                  X2 -> X1
    ^                               ^
    |                               |
   root D3, root D1                root X2

An unresolved ``D3`` does not prevent ``X2`` from ticking.  A publisher which
depends on inputs from both forests sees their independently advancing current
versions and records the combination present when it evaluates.

If a new revision changes dependencies, forests may merge or split.  A merge
does not retract values already delivered; it subjects the pending frontier to
the new combined consistency constraints.  A split lets the resulting forests
advance independently from then on.

Consistency rule
~~~~~~~~~~~~~~~~

A candidate cut is consistent when:

* every selected ordinary dependency can be resolved to a revision carrying
  the required output version;
* whenever two ordinary paths require the same data id at the current
  frontier, they require the same data version;
* every referenced Frame and revision exists and validates;
* the selected schema for each id matches its fixed schema; and
* the ordinary dependency closure is acyclic.

A historical self-predecessor may contain an older version of the same id but
does not enter the current-frontier equality test.

Independent roots are consistent by independence, not by sharing an epoch.
As-of values help order replay but are not a substitute for lineage.

Rolling ancestry
~~~~~~~~~~~~~~~~

Suppose output version 7 of ``A`` appears in revisions 10, 11 and 12 while
``A`` acknowledges newer inputs without changing its Frame.  A dependency on
``A:7`` tries revision 12 first.  If its ancestry conflicts with another root,
the resolver may try revision 11 and then 10.  Thus rolling ancestry means
*newest compatible revision*, not blindly newest revision.

The subscriber must therefore observe revision changes to ``A`` even when
``A`` is only a transitive dependency and its output version remains 7.  Such
a revision can change the selected closure and release a held changed root;
the unchanged transitive Frame is not loaded into the graph.

This choice lets a same-version acknowledgement unlock a newer parent without
rewriting every downstream revision which already names that data version.  It
also means the full transitive ancestry of a version-level dependency is a
rolling view.  Applications requiring an immutable historical proof must
record the resolved cut itself; dependency revision ids are intentionally not
part of this v1 contract.

Resolver state machine
~~~~~~~~~~~~~~~~~~~~~~

One forest resolver returns:

``Ready``
   A unique greatest consistent cut exists and at least one root data version
   is newer than the exposed version.

``Unchanged``
   A consistent cut exists but no root data version changed.  Selected
   revisions and hidden lineage metadata may still advance.

``Pending``
   Accepted state is valid, but a compatible acknowledgement has not arrived,
   or a detected revision gap is still being recovered.  The previous exposed
   cut remains active.  An accepted revision never becomes ``Pending`` merely
   because an immutable object which it references is absent.

``Ambiguous``
   Consistent closures exist but none is a unique greatest closure under the
   component-wise revision ordering.  The forest is held and diagnosed rather
   than choosing by data-id spelling.

``Cyclic``
   Ordinary dependency expansion found a cycle.  The affected forest is in
   error; historical self-predecessors are excluded from this check.

``Corrupt``
   An immutable object required by an accepted revision is missing, fails
   persistence integrity or fixed-schema validation, has a malformed
   path/record, or violates monotonic revision invariants.  Missing or stale
   derived ``latest`` and as-of indexes are repaired from contiguous accepted
   revision slots where possible rather than reclassifying the accepted
   history.

``Pending``, ``Ambiguous``, ``Cyclic`` and ``Corrupt`` affect only publishers
and root inputs which depend on that forest.  Other forests continue.

Resolver algorithm
~~~~~~~~~~~~~~~~~~

The normative search is depth-first backtracking with newest-first candidates.
An implementation may optimise or memoise it only if it returns the same cut:

.. code-block:: text

   resolve_forest(root_ids, observed_heads, previous_cut):
       candidates = newest revisions at or below each observed root head
       solutions = []

       search(selected, frontier_requirements):
           if an ordinary dependency cycle is present:
               return Cyclic

           if one frontier data id is required at two versions:
               backtrack

           if every requirement has a selected compatible revision:
               solutions += closure(selected)
               return

           requirement = choose_unresolved_requirement()

           for revision in revisions_newest_first(requirement.data_id):
               if revision.output_version != requirement.version:
                   continue
               if revision is older than the previous exposed choice where
                  that would regress an already delivered root version:
                   continue

               select revision
               add its ordinary dependency version requirements
               ignore its self_predecessor for consistency resolution
               search(selected, frontier_requirements)
               undo selection

       search({}, root requirements from observed heads)

       if an accepted revision or immutable object it names is absent:
           return Corrupt
       if no solution because of validated incompatibility: return Pending

       greatest = solution which component-wise dominates every other
       if no unique greatest: return Ambiguous
       if no root output version changed: return Unchanged(greatest)
       return Ready(greatest)

Root revisions are candidates too.  This is what permits the D1/D3 example to
move direct ``D1`` back from revision/output 3 to revision/output 2.  A running
resolver never retracts an exposed root data version; its previous cut remains
a lower bound while later notifications are pending.

Practical implementations keep indexes from output version to the newest
known revision ranges, cache immutable records, and invalidate only the forest
connected to a changed id.  They must not scan the complete retained history
on every notice.

Delivery into the graph
~~~~~~~~~~~~~~~~~~~~~~~

When a forest returns ``Ready`` the coordinator loads each changed direct
Frame and publishes all changed roots in one graph evaluation cycle.  A root
whose selected revision advanced but whose output version is unchanged does
not tick.  Its hidden lineage selection is updated atomically with the cut.

This makes the common held-parent sequence explicit:

1. direct parent ``D1`` version 3 arrives;
2. direct child ``D3`` still resolves through ``D1`` version 2;
3. the forest remains on its prior cut;
4. ``D3`` publishes a revision acknowledging ancestry compatible with
   ``D1`` version 3 while retaining its own output version;
5. the new forest cut becomes ready;
6. ``D1`` version 3 ticks, while unchanged ``D3`` does not; and
7. publishers depending on ``D1`` observe the changed input tuple even if
   their own output does not tick.

Publishers sharing a subscription see the same input tick.  Each publisher's
hidden dependency projection independently schedules its revision handling.
There is no need to duplicate or exclusively claim the subscription.

Subscription modes
------------------

Explicit selection
~~~~~~~~~~~~~~~~~~

Every subscription declares ``Live``, ``Replay`` or ``Snapshot`` explicitly.
The operator dispatch mechanism selects the concrete graph/source overload at
wiring time, when the mode scalar is known.  There is no automatic executor
mode mapping and no generic source which changes ingress strategy at start.
This keeps the real-time wake path structurally separate from deterministic
scheduled replay and snapshot sources.

Live mode
~~~~~~~~~

Live startup is a no-gap handoff:

1. establish the keyed Kafka subscription and its start position;
2. read ``latest`` for every configured root id;
3. recursively load the latest records for every data id reached through their
   ordinary dependencies and begin watching those ids;
4. repair contiguous accepted revision/index state found ahead of a stale
   pointer;
5. resolve and emit the newest consistent initial cut in each forest;
6. drain and conflate notices received during the durable read; and
7. enter ordinary notification-driven operation.

The initial image ticks at graph start.  Later cuts tick when the notification
which makes them ready is admitted to the graph.  A slow subscriber is allowed
to skip intermediate versions and move directly to the newest consistent cut.

After handoff, no periodic object-store polling is required.  Kafka production
must be acknowledged, and reconnect repeats the handoff.  If an installation
cannot provide that transport contract it must not claim uninterrupted live
updates; durable startup recovery still provides the latest state.

Replay mode
~~~~~~~~~~~

Simulation uses the as-of indexes, not Kafka.  For the executor's half-open
graph interval ``[start_time, end_time)``:

1. find the greatest revision at or before ``start_time`` for each root and
   every ancestry record required to resolve it;
2. resolve each forest and emit its initial consistent image at
   ``start_time``;
3. perform a k-way chronological merge of later as-of entries for the root ids
   and every id in their dynamically reachable transitive ancestry;
4. apply all entries sharing an as-of timestamp before resolving affected
   forests;
5. emit a newly ready cut at the as-of time of the revision which made it
   ready; and
6. stop before the first entry whose as-of is greater than or equal to
   ``end_time``.

If ``start_time`` is ``MIN_ST``, there may be no seed and replay begins with
the first published revisions.  If a revision advances lineage without
changing a direct output version it does not tick by itself, but it can make a
held changed version tick at that revision's as-of.

As-of is strictly increasing only within a data id.  Equal times across ids are
batched.  Clock skew can change the historical time at which a distributed cut
becomes knowable, but cannot make the resolver expose an inconsistent cut.
Every revision candidate used at replay time ``T`` must itself have
``as_of <= T``; rolling ancestry may not select a future acknowledgement merely
because it exists when the replay is executed.  If a revision introduces a new
dependency id, the replay merge opens that id's as-of history at the current
time and incorporates only entries which were then knowable.

Snapshot mode
~~~~~~~~~~~~~

``Snapshot`` requires an explicit ``as_of``.  It selects the greatest
consistent cut whose indexed revisions are not later than that time and emits
the selected direct Frames once at graph start.  It does not move the graph
clock to the historical time and it does not follow later Kafka or store
updates.  The time bound applies recursively to every revision selected in the
ordinary dependency closure.

Worked examples
---------------

Scheduled dataset chain
~~~~~~~~~~~~~~~~~~~~~~~

Consider three independently deployed graphs:

.. code-block:: text

   source schedule             normaliser                 report
   ----------------            ----------                 ------
   publish D1          --->    subscribe D1       --->    subscribe D3
                               publish D2                  subscribe D1
                                  |                        publish C
                                  v
                               subscribe D2
                               publish D3

The source graph can finish before the normaliser starts.  ``D2`` can take much
longer than ``D1``.  The report graph starts from durable ``D1`` and ``D3`` and
does not call either upstream graph.

D1/D2/D3 bootstrap
~~~~~~~~~~~~~~~~~~

The report subscribes directly to ``D1`` and ``D3``.  Storage contains:

.. code-block:: text

   R3-D1: dependencies {},       output 3
   R2-D3: dependencies {D2: 2},  output 2
   R3-D2: dependencies {D1: 3},  output 3
   R2-D2: dependencies {D1: 2},  output 2
   R2-D1: dependencies {},       output 2

The resolver begins at ``R3-D1`` and ``R2-D3``.  ``D3`` requires ``D2`` output
2, so it skips ``R3-D2`` and selects ``R2-D2``.  That requires ``D1`` output 2,
which conflicts with direct ``R3-D1`` output 3.  It backtracks the root ``D1``
candidate to ``R2-D1``.  The initial direct cut is therefore:

.. code-block:: text

   D1: version 2
   D3: version 2

Only those two direct Frames are loaded; ``D2`` metadata proves the ancestry
but its Frame is not a root graph input.

Unchanged output acknowledgement
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now ``D1`` publishes version 3.  It remains held because ``D3`` still depends
on the ``D2`` version whose lineage requires ``D1`` version 2.  ``D2`` then
publishes output version 3, and ``D3`` evaluates that change but retains output
version 2:

.. code-block:: text

   R3-D3: dependencies {D2: 3}, output 2

Rolling ancestry now resolves ``D2:3`` through ``D1:3``.  The direct cut
becomes ``D1:3, D3:2``.  ``D1`` ticks; ``D3`` does not.  The report still
evaluates because one of its inputs changed.

Independent forests
~~~~~~~~~~~~~~~~~~~

If the same report also subscribes to unrelated ``X``, the ``X`` ancestry is a
second forest.  A missing ``D2`` revision can hold the D forest while ``X``
continues to tick and drive report logic which depends on it.  There is no
fabric-wide epoch and no reason to combine unrelated latest values into one
barrier.

Several independent forests can tick in one engine cycle if their notices are
admitted together.  They remain independently resolved; co-scheduling does not
invent lineage between them.

Input change without output change
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Component ``C`` depends on ``A:10`` and publishes output version 20.  ``A:11``
ticks, so ``C`` evaluates, but its computation returns no output tick.
``publish_data`` is nevertheless scheduled by the hidden input-cut change and
publishes:

.. code-block:: text

   previous: dependencies {A: 10}, output 20
   new:      dependencies {A: 11}, output 20

No Frame is copied.  Downstream components can now establish that ``C:20`` is
compatible with ``A:11``.

Scheduled output without input change
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A time-driven component can tick a new Frame while all fabric input versions
remain unchanged.  It allocates a new output version and revision using the
same dependency tuple.  Timer identity is not added to lineage; the Frame and
publication as-of show that a new result was published.

Shared subscription
~~~~~~~~~~~~~~~~~~~

Two publishers in one graph both use a single ``subscribe_data("A")`` result.
When ``A`` ticks, the subscription emits once.  If both publishers' dependency
sets include that port, each records ``A``'s current version in its own next
revision.  Their output decisions and durable writes remain independent.

Scheduled initialiser and live updater
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A scheduled process may periodically publish a complete dataset while a live
updater publishes a more current complete image between schedules.  They share
one data id but normally do not write concurrently.  During overlap both
prepare against the current head; the first immutable next-revision write wins
and the other attempt is dropped.

If the live updater builds version 9 from previously published version 8, its
revision records ``self_predecessor=8``.  That history is inspectable, but a
subscriber exposes only version 9 and does not treat the predecessor as an
ordinary consistency constraint.

Slow live subscriber
~~~~~~~~~~~~~~~~~~~~

A subscriber is processing ``A:10`` while notices for revisions producing
``A:11``, ``A:12`` and ``A:13`` arrive.  Its notification queue retains only
the newest data-id wake-up.  The resolver reads the accepted head and moves to
the newest consistent cut containing ``A:13``.  Versions 11 and 12 remain in
durable history and appear in simulation replay, but need not tick in this live
run.

Failure and diagnostics
-----------------------

Fabric failures are data-boundary failures rather than silent log messages.
The implementation must expose path-addressed diagnostics through the normal
node error/status facilities and log lifecycle context.  At minimum it must
distinguish:

* missing data, revision, as-of or latest objects;
* incomplete first publication;
* malformed metadata and unsupported format versions;
* persistence integrity and fixed-schema failure;
* non-contiguous or non-monotonic revision history;
* ordinary dependency cycles;
* ambiguous maximal cuts;
* persistence authentication, availability and conditional-write failures;
* Kafka production acknowledgement failure;
* Kafka subscription/rebalance/disconnect state; and
* startup handoff failure.

A detected revision gap or a valid cut waiting for a compatible acknowledgement
produces ``Pending`` and may resolve when the missing accepted history arrives.
Once an accepted revision exists, a missing Frame or immutable ancestry record
which it references produces ``Corrupt``; it is not held indefinitely and is
not bypassed by falling back silently to an older latest value.  Missing or
stale derived indexes are repaired when contiguous accepted revision slots
prove the correct value.  Independent forests continue in all cases.

The extension must expose enough metrics to observe notification lag, durable
read latency, pending-forest age, resolver backtracking, conflated notices,
publication races and repair operations.  The exact metrics integration is an
implementation choice, not a new core API.

Determinism and side effects
----------------------------

Fabric consistency does not prove computation determinism.  Component authors
declare the following operating contract:

* repeated computation over the same fabric inputs produces an equivalent
  result;
* external observations which affect the result are controlled or accepted as
  outside fabric lineage;
* a side effect is safe to repeat, transactionally protected, or explicitly
  deduplicated by the component; and
* a losing publication race can be discarded without requiring that attempt
  to become the next revision.

The fabric makes its own operations idempotent: immutable writes can be
retried, a repeated accepted tuple does not create another revision, duplicate
Kafka notices are harmless, and startup can repair derived indexes.  It does
not suppress or roll back arbitrary side effects performed by user graphs.

Performance and memory
----------------------

``eval`` remains the hot path for ordinary hgraph nodes and gains no fabric
branch, lineage token or provenance wrapper.

Fabric costs occur at explicit boundaries:

* dependency discovery is wiring-time graph inspection;
* a subscribed notice schedules one affected-forest resolution;
* revision records are cached because they are immutable;
* an output Frame is serialised only when it ticks;
* an unchanged-output acknowledgement writes metadata only;
* the root Kafka service uses its standard burst push-source queue, while the
  Fabric live session admits and conflates only data ids in the observed
  consistency forest; and
* unchanged direct Frames are not read or ticked again.

Resolver worst-case work is exponential in the number of conflicting candidate
revision choices.  Practical histories should normally resolve newest-first
without backtracking, but the implementation must report:

* revisions and dependency edges examined per resolution;
* cache hits/misses;
* maximum and average backtracking depth; and
* time from notice to ready cut.

Indexes from ``(data id, output version)`` to revision ranges and invalidation
by affected forest bound ordinary work.  A benchmark must include long runs in
which an output version remains unchanged across many revisions; scanning that
whole run for every dependency lookup is not acceptable.

The retained durable footprint is intentionally unbounded in v1: one complete
Frame per output tick plus one small revision/as-of entry per accepted input or
output tuple.  The RFC trades storage for exact replay and operational
simplicity.  This cost must be explicit in deployment documentation.

Accepted performance evidence
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The accepted measurements used a GCC 14.2 Release build in the x86_64 Ubuntu
VM, pinned to one CPU.  Resolver figures are medians of nine warm cached
resolutions.  Work counters are from the selected median-equivalent scenario;
the cache-miss assertion was zero for every warm sample.

.. list-table:: Resolver measurements
   :header-rows: 1
   :widths: 31 10 14 15 15 15

   * - Scenario
     - Scale
     - Median
     - Revisions
     - Edges
     - Max depth
   * - unchanged-output rolling ancestry
     - 2,048 revisions
     - 4.081 ms
     - 2,050
     - 2,049
     - 3
   * - broad ancestry
     - 128 leaves
     - 6.467 ms
     - 129
     - 128
     - 129
   * - deep ancestry
     - 256 levels
     - 27.336 ms
     - 256
     - 255
     - 256
   * - conflict-heavy ambiguous ancestry
     - 24 revisions
     - 0.185 ms
     - 624
     - 600
     - 3

The 1,049,194-byte, 65,536-row/two-column Frame benchmark measured 17,213
MiB/s for Arrow IPC serialisation.  Combined immutable put plus get against the
memory object-store strategy measured 520 MiB/s.  Both are nine-sample medians
over twenty operations per sample and report a checked payload-size checksum.

Non-Fabric impact was measured separately with the 53-case type-erasure
campaign against a fresh ``main`` build, using the same compiler, source/build
paths and CPU pinning.  Benchmark inventory, iterations, allocation counts,
allocated bytes and checksums matched exactly.  The ordinary graph paths met
the five-percent gate: erased native-node evaluation was 1.38 percent faster,
disabled/enabled profiler cycles were 5.90/1.87 percent faster, small-graph
construction was 2.38 percent faster, nested scheduled scan was 2.58 percent
slower, and dynamic TSL map/map-reduce cycles were 1.74/0.83 percent slower.
Sparse TSD map, reduce and combined cycles improved by 17.55--19.80 percent.

The raw whole-inventory timing comparator also reported repeatable outliers in
two isolated value-dispatch microbenchmarks: external polymorphic-union
copy/hash (11.90 percent) and bundle visitor dispatch (17.77 percent).  Their
implementation sources and generated helper bodies were unchanged; only
indirect-call target placement differed after relinking the much larger stack.
They are recorded as linker-layout-sensitive results rather than evidence for
an ordinary graph evaluation regression.  The accepted performance claim is
therefore deliberately limited to the graph evaluation paths above, not a
claim that every sub-35-nanosecond microbenchmark passes a raw binary-to-binary
five-percent timing threshold.

Compatibility and migration
---------------------------

The feature is additive.  Core hgraph, ``hgraph-persistence`` and
``hgraph-kafka`` retain their existing contracts.  A core-only installation
does not expose ``publish_data`` or ``subscribe_data`` and gains no dependency
on fabric.

The old experimental ``hgraph.adaptors.data_fabric`` surface was never a
released C++-first contract and is not revived as a compatibility alias.  If a
downstream project currently uses that experiment it migrates deliberately to
``hgraph_fabric``; dynamic atom construction and durable data publication are
different abstractions.

V1 Frame schemas are fixed by data id.  There is no transparent migration from
one schema to another under the same id.  A producer publishes a new id and
consumers migrate through ordinary application configuration.

Alternatives considered
-----------------------

Direct request/reply services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Request/reply remains appropriate when a caller needs a correlated answer from
a currently available service.  It couples availability and latency and does
not by itself preserve the current result or its input lineage.  The fabric is
for materialised recurring state.

Kafka payloads as the durable source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Putting complete Frames only on Kafka would require broker retention and
consumer replay to reconstruct current state, make large images part of the
notification path, and duplicate the object-store history required for
simulation.  Kafka therefore carries revision metadata while persistence
stores Frames.

Delta publication
~~~~~~~~~~~~~~~~~

Deltas reduce bandwidth for frequently changing large values, but they require
unbroken sequencing, initial images, compaction, slow-client policy and more
complex recovery.  That is the separate cached subscription/data-distribution
problem.  This RFC deliberately starts with complete atomic values.

One global consistency barrier
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Holding every root input until every other root is consistent would let one
broken dataset stop unrelated computation and would invent a relationship
between independent data.  Connected ancestry forests are the appropriate
unit.

Revision-qualified dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Storing ``(data version, revision)`` on every dependency would give immutable
transitive historical ancestry.  It would also require downstream revisions
to propagate every same-version acknowledgement explicitly.  Version-only
dependencies plus newest-compatible rolling ancestry support the desired
acknowledgement behavior with smaller records and less propagation.

Content equality for version allocation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Hashing a newly emitted Frame to suppress an equal version would make
publication decide whether the graph really ticked.  Here the graph output
tick is authoritative.  Checksums validate transport; they do not define
semantic equality.

Multiple legitimate active writers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A merge protocol, leader election or total writer order would be required if
different active writers were expected to publish every competing result.
The operating assumption is single active ownership with brief or specialised
overlap, so first next-revision write wins and a loser is discarded.

Central orchestration
~~~~~~~~~~~~~~~~~~~~~

Kubernetes, process supervisors and application deployment systems already
own placement and lifecycle.  Adding a scheduler would obscure the useful
data-plane contract and is not required for durable decoupling.

Security and operational configuration
--------------------------------------

Fabric objects can contain sensitive datasets and lineage.  The implementation
inherits credentials and encryption policy from ``hgraph-persistence`` and
Kafka authentication from ``hgraph-kafka``.  It must not place credentials in
Frame metadata, revisions, Kafka notices or logs.

Deployments should scope object and topic permissions to the fabric prefix,
enforce conditional writes on immutable and latest keys, encrypt transport and
storage, and restrict administrative listing separately from the data ids a
component consumes.  Data-id strings are validated before becoming paths or
log fields.

S3 supports conditional creation and compare-by-ETag writes using
``If-None-Match`` and ``If-Match``; the persistence implementation must expose
those semantics rather than emulating them with a read followed by an
unconditional write.  Amazon documents both the first-finishing concurrent
write behavior and strong whole-object visibility:

* `S3 conditional writes
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html>`_;
* `S3 consistency model
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel>`_;
  and
* `S3 PutObject
  <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>`_.

Implementation outcome
----------------------

The implementation landed through the separately reviewed checkpoints tracked
by issue 512.  It supplied the reusable persistence object-store contract,
installed-SDK extension seam, Fabric public types and operators, durable
publication state machine, wiring planner, resolver/coordinator, all three
subscription modes, production Kafka adapter, backend/package matrix,
diagnostics and performance evidence.

The accepted implementation resolves the proposal's remaining ownership and
lifecycle choices as follows:

* One lazy root ``FabricServiceImpl`` graph composes Fabric publication,
  snapshot, replay, live, synchronous load, diagnostics and lifecycle nodes.
  Each node owns its local algorithm state in its graph ``State`` slot; the
  immutable wiring plan is copied into each planned node's ``State`` at node
  start, and persistence handles
  come from run-scoped ``GlobalState``.  Mutable orchestration is not shared
  through a service-runtime object.  Nested and root clients use
  purpose-specific service interfaces to that graph.
* The optional Kafka adapter is a separate lazy service graph.  Each
  ``GraphValue`` likewise owns its Kafka runtime and broker workers.  It uses
  the RFC 0015 standard burst transport and root real-time push source.  Its
  drain node emits ordinary graph edges into Fabric; broker callbacks never
  address the graph directly.
* Durable publication candidates, one-at-a-time notification dispatch,
  delivery correlation, retry, completion feedback and their counters are
  modelled by Fabric graph nodes and time-series edges.  The outgoing edge is a
  reference to the retained ``Shared<DataRevision>`` endpoint; retry rebinds
  that reference without materialising another value.  Kafka owns only the
  publish sink and ordered push-source return boundary.
* V1 Frame loading is synchronous through the service-owned request/reply
  contract.  It therefore has no Fabric worker/completion queue or in-flight
  load de-duplication.  A future asynchronous strategy must return completions
  through one service-owned root push-source edge; that strategy is not part of
  V1.
* Kafka carries the complete accepted ``DataRevision`` keyed by data id.  A
  valid notice directly populates revision, output-version and dependency
  indexes.  Persistence metadata is read for startup, reconnect, detected gaps
  and uncached selected Frames rather than once per notice.
* ``Live``, ``Replay`` and ``Snapshot`` are required wiring-time choices.
  Replay and Snapshot are ordinary deterministic scheduled sources; every push
  source remains real-time-only.
* Metadata uses the canonical version-1 ``HGFM`` binary envelope and the media
  types declared in ``metadata_codec.h``.  Public contracts are split across
  ``types.h``, ``config.h``, ``operators.h``, ``service.h`` and the persistence
  and Kafka adapter headers.
* Diagnostics are a bundle of stable string metrics and typed, path-addressed
  events.  Publication, live notices, notification requests, retries and
  diagnostic paths have the public hard bounds documented by the extension.
* The standalone Fabric package and wheel depend on core and Persistence but
  remain Kafka-free.  ``hgraph::fabric_kafka`` is an optional native target so
  the production transport does not leak into the base dependency boundary.

The checkpoint-0 proof supplied the generally reusable compiled-child
inspection hook described above.  It lives under the shared C++ wiring path, so
Python-authored graphs which lower through that path receive the same behavior
without a second Python runtime implementation.  Fabric-specific policy stays
in the extension.

Acceptance criteria and test plan
---------------------------------

Native C++ behavior is the source of truth.  Python tests exercise the same
operators and serialized schemas.  At minimum the implementation must cover:

Public contract
~~~~~~~~~~~~~~~

* first-class C++ and Python wiring for publish, live subscribe, replay and
  snapshot;
* installed-SDK C++ and Python consumers;
* registration across registry reset;
* invalid modes, ids, prefixes and missing as-of arguments; and
* rejection of two publishers for one data id in one wired graph.

Publication
~~~~~~~~~~~

* first publication and fixed-schema establishment;
* every output tick allocating a monotonically increasing version;
* input-only revision with unchanged output version;
* scheduled output with unchanged dependencies;
* duplicate accepted tuple suppression;
* first-writer-wins process overlap and unconditional loser drop;
* idempotent same-object retry and conflicting same-millisecond version writes;
* every crash boundary in the publication table;
* stale latest/as-of repair;
* Kafka messages carrying only the accepted durable winner;
* delivery retry without changing or rolling back that winner; and
* targeted durable gap recovery without a slot read for every valid message.

Resolution
~~~~~~~~~~

* the D1/D2/D3 bootstrap example;
* newest-compatible rolling ancestry across many same-version revisions;
* held-parent release by an unchanged-output child revision;
* independent forests advancing and failing independently;
* forest merge and split after dependency-set changes;
* shared subscriptions driving several publishers;
* historical self-predecessor without an ordinary cycle;
* ordinary cycle, missing immutable accepted ancestry reported as corrupt,
  corrupt Frame, schema mismatch and ambiguous maximal cuts; and
* no regression of an already exposed root version.

Modes and lifecycle
~~~~~~~~~~~~~~~~~~~

* live initial image and notification handoff with updates at every race point;
* explicit wiring-time selection of ``Live``, ``Replay`` and ``Snapshot``;
* no push source in deterministic replay or snapshot graphs;
* duplicate, stale, invalid, out-of-order and conflated Kafka messages;
* slow clients skipping intermediate versions;
* reconnect reconciliation;
* simulation seed at a non-minimum start time;
* replay from ``MIN_ST``;
* equal as-of timestamps across data ids;
* a held cut ticking at the revision time which makes it ready;
* end-time exclusion; and
* snapshot selection and its one tick at graph start.

Storage and platforms
~~~~~~~~~~~~~~~~~~~~~

* identical behavior for memory, local and S3 store strategies;
* local and S3 conditional-write races;
* Arrow IPC and supported Parquet Frame formats;
* actual Kafka integration including broker restart and producer
  acknowledgement failure; and
* complete native and Python 3.14 non-WIP suites on macOS and Linux, with
  opportunistic Windows validation.

Performance gates
~~~~~~~~~~~~~~~~~

* no measurable regression for graphs which do not use fabric;
* no per-node lineage propagation through ordinary compute evaluation;
* bounded notification queue memory under a stalled graph;
* cached lookup of long same-output-version revision runs;
* resolver measurements for broad, deep and conflict-heavy ancestry; and
* Frame serialisation and object-store throughput reported separately from
  graph evaluation cost.

Resolved implementation details
-------------------------------

The representation choices left open by the proposal are fixed by the public
installed headers and canonical codec:

* registered Bundles use the ``hgraph.fabric::`` names shown above and live in
  the focused public header split recorded under *Implementation outcome*;
* revision, latest and as-of values use the big-endian ``HGFM`` version-1
  envelope and the ``application/vnd.hgraph.fabric.*.v1+binary`` media types;
* Persistence owns the type-erased ``persistence::store::ObjectStore`` and
  ``FrameStore`` handles consumed by ``FabricConfig``;
* resolver outcomes use the typed ``ResolutionStatus`` values, while the
  service exposes stable metrics plus typed ``FabricDiagnosticEvent`` values;
  and
* Fabric configuration owns ``prefix``, ``objects``, ``frames`` and the base
  ``notifications`` handle; Kafka topic and client configuration belong to the
  optional adapter registration.

Changing version/revision meaning, forest independence, rolling ancestry,
live conflation, replay timing, permanent history, publication ordering or
first-writer-wins behavior requires updating this RFC before implementation is
accepted.

Relationship to other RFCs
--------------------------

* :doc:`rfc_0001_typed_frame_metadata` — Arrow Frame schema metadata preserved
  by persisted payloads.
* :doc:`rfc_0014_request_reply_transport_planning` — transport planning used by
  extension services, though fabric itself is state distribution rather than
  request/reply.
* :doc:`rfc_0015_kafka_extension_api` — the production notification transport.
* :doc:`rfc_0016_object_store_frame_persistence` — the original FrameStore
  semantics now owned by the persistence extension.
* :doc:`rfc_0017_binary_value_codec` — a possible future codec for compound
  scalar payloads; not required for Frame-only v1.
* :doc:`rfc_0021_recording_versions` — related version-history questions in
  record/replay, without sharing fabric semantics.
* :doc:`rfc_0023_graph_checkpoint_recovery` — the reusable immutable object and
  conditional-reference store requirements.
* :doc:`rfc_0025_hgraph_persistence` — the extension which owns durable store
  implementations and policy.
* :doc:`research_layered_network_services` — separates request/reply, lossless
  streaming and conflated cached-state distribution.
