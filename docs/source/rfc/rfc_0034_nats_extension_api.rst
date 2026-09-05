RFC 0034: C++-First NATS and TSD Cache Extension API
====================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-09-02
:Target: NATS extension, NATS Key/Value-backed TSD caches, public C++ SDK,
         and Python authoring surface

Summary
-------

Add a separately installed, C++-first ``hgraph-nats`` extension backed by the
official NATS C client.  One graph-scoped ``NatsServiceImpl`` owns one NATS
connection, its JetStream context, subscriptions, request multiplexing, and
callbacks for a service path.  Registration supplies immutable configuration
and materializes the implementation lazily; it performs no network work while
the graph is wiring.

The public API deliberately keeps Core NATS, JetStream event delivery, and
NATS Key/Value cached state distinct.  They have different persistence,
acknowledgement, flow-control, and loss contracts:

.. list-table::
   :header-rows: 1
   :widths: 24 28 48

   * - Operation
     - hgraph contract
     - Observable meaning
   * - Core subscribe
     - subscription service
     - Receive currently published messages matching a subject filter; Core
       NATS remains at-most-once and has no replay or acknowledgement.
   * - Core publish
     - request/reply service
     - Receive an attributed local acceptance or rejection report.  Acceptance
       means the NATS client accepted the message, not that any subscriber
       received it.
   * - Core request
     - request/reply service
     - Receive the first response or a typed no-responder, timeout, cancellation,
       rejection, or transport result.
   * - JetStream consume
     - subscription service
     - Pull durable messages with explicit acknowledgement tokens and server
       metadata.
   * - JetStream publish
     - request/reply service
     - Receive the JetStream publish acknowledgement or an attributed failure.
   * - JetStream acknowledge
     - request/reply service
     - Send ``Ack``, ``Nak``, ``Term``, or ``InProgress`` only after graph work
       reaches the acknowledgement call, with an optional server-confirmed
       result.
   * - Key/Value watch
     - subscription service
     - Receive the latest entry for every matching key as an initial snapshot,
       an explicit snapshot/live boundary, and then live puts, deletes, purges,
       and expiry markers.
   * - Key/Value mutate
     - request/reply service
     - Put, create, compare-and-swap update, delete, or purge one byte-valued
       key and receive its attributed revision or typed failure.
   * - Service events
     - reference service
     - Observe connection, reconnect, slow-consumer, protocol, and lifecycle
       events shared by the service path.

The nine interfaces are registered atomically against one multi-interface
service implementation.  External callbacks and pull workers enter a
real-time graph only through one standard bounded burst push source carrying
fully owned discriminated envelopes.  One graph-side emitter preserves the
admitted order while projecting Core messages, request results, JetStream
messages, publish acknowledgements, acknowledgement reports, KV entries,
snapshot barriers, KV mutation reports, and events onto their public service
outputs.  Repeated output keys are released on successive engine cycles rather
than conflated.

The primary cached-state API is typed composition over the two byte-level KV
services:

* ``publish_tsd<K,V>`` exposes the current state of a
  ``TSD<K,V>`` beneath a bucket/cache namespace; and
* ``subscribe_tsd<K,V>`` materializes that namespace as a local
  ``TSD<K,V>`` plus explicit warming, live, recovering, stale, and failed
  state.

Each KV entry holds the **current image of one TSD child**, not its last delta.
The cache is therefore state-lossless but event-lossy: after a successful
image or recovery, a connected consumer converges to the publisher's latest
acknowledged per-key state, while obsolete intermediate changes may be
conflated.  A generation/head protocol makes initial attach, publisher restart,
and re-image replace the consumer's TSD atomically.  Live changes to different
keys are independent KV operations and do not preserve one source engine
cycle as an atomic remote tick.  Applications needing tick-accurate replay use
JetStream image/delta frames instead of this cache contract.

The initial extension is live and real-time only.  A simulation graph rejects
the live implementation.  Deterministic replay requires a separate recorded
input contract and is not approximated by allowing a broker callback to wake a
simulation executor.

Motivation
----------

NATS covers two useful but materially different hgraph integration needs.
Core NATS is a low-latency subject router: a publication goes to subscribers
which are interested at that moment, and a publication with no interest is
discarded.  It offers wildcard subscriptions, queue-group load balancing,
headers, and request/reply over reply subjects, but no storage or subscriber
acknowledgement.  JetStream adds retained streams, server-side consumers,
publish acknowledgements, explicit consumer acknowledgements, redelivery, and
bounded pull consumption.

NATS Key/Value adds a third need which is central to this RFC: distributed
materialized state.  A KV watch supplies the latest value of every matching key
followed by live changes, with an explicit end-of-initial-data boundary.  That
shape maps naturally to a ``TSD`` if each NATS key represents one TSD key and
each value represents the current child image.  It avoids rebuilding a full
TSD for every subscriber and lets the broker retain the working set while no
consumer is attached.

These properties fit hgraph's existing services well:

* subscription keys naturally identify Core subject filters and JetStream
  consumers;
* request/reply services already correlate independent publishers, unary NATS
  requests, publish acknowledgements, and acknowledgement reports by stable
  client id;
* a reference service exposes connection and runtime events once per path;
* a root push source is the existing safe boundary from NATS-owned callbacks
  and pull workers into a real-time graph; and
* explicit graph-to-NATS acknowledgement calls keep protocol confirmation
  downstream of the work the graph considers successful;
* a KV watch's snapshot boundary is a natural graph-side image barrier; and
* KV revisions and compare-and-swap provide attributed mutation results and a
  safe generation/head commit for a TSD cache.

A Python-only adaptor would violate the repository's C++-first direction,
duplicate lifecycle and queue semantics, and acquire the GIL on the byte
transport path.  A generic messaging abstraction would be premature: Core
NATS, JetStream, and Kafka differ in persistence, addressing, cursor identity,
acknowledgements, ordering, and overflow behavior.  The extension should use
the common hgraph service boundary without pretending that the broker
contracts are interchangeable.

Protocol review
---------------

Core NATS
~~~~~~~~~

Core NATS is an ephemeral publish/subscribe system.  A subscriber sees a
message only while it is connected and has matching interest.  Delivery is
at-most-once; there is no broker replay and no consumer acknowledgement.
Wildcard subject filters use token-aware ``*`` and ``>`` matching, while a
publisher names a concrete subject.  A queue group load-balances each matching
message to one member of the group rather than broadcasting it to all group
members.

Core request/reply is ordinary publish/subscribe with a unique reply inbox.
The normal request operation takes the first reply.  A server which supports
headers can report that a subject has no responders immediately with a 503
status message; this is observably different from a timeout.  Scatter/gather
uses a different multi-reply lifecycle and is outside the initial contract.

Core publish success is not broker or application delivery.  The client may
buffer outbound data, including during reconnect.  A flush proves that the
server observed all writes before the flush round trip, but still says nothing
about subscriber processing.  The initial API therefore reports only local
publish acceptance and rejects any ``Delivered`` interpretation.

Slow Core subscribers are lossy.  The NATS client has per-subscription pending
message and byte limits; exceeding them produces a slow-consumer error and can
drop inbound messages.  The extension can bound and report this condition but
cannot turn Core NATS into a durable transport.

JetStream
~~~~~~~~~

JetStream stores messages in streams and delivers them through consumers.  An
explicit-ack consumer keeps a message pending until it receives a terminal
acknowledgement.  ``Nak`` requests redelivery, optionally after a delay;
``Term`` ends delivery without claiming successful processing; and
``InProgress`` extends the acknowledgement deadline without completing the
message.

The initial extension uses pull consumers only.  Pull batch count, byte count,
and expiry bound each fetch, while ``MaxAckPending`` bounds delivered but
unacknowledged messages across all workers sharing a consumer.  This composes
directly with hgraph's bounded sender: a pull owner stops fetching while graph
admission is blocked, without inventing a second extension ingress queue.

JetStream publish acknowledgement is distinct from Core publish acceptance.
The acknowledgement identifies the stream and sequence and reports whether a
stable message id was treated as a duplicate.  Asynchronous publish failures
and retries may reorder messages; the extension exposes message-id and expected
sequence controls but does not hide retry behind an unconditional policy.

JetStream's acknowledgement and de-duplication features do not by themselves
make an hgraph pipeline exactly once.  End-to-end exactly-once processing also
requires checkpointed graph state, side-effect coordination, restoration, and
an atomic protocol spanning the source and destination.  This RFC makes no
such claim.

NATS Key/Value
~~~~~~~~~~~~~~

A NATS KV bucket is a materialized view over a JetStream stream.  ``Put``
appends a value, ``Get`` reads the latest entry for one key, and ``Watch``
opens an ordered ephemeral consumer.  A normal watch delivers the latest entry
for every matching key, one end-of-initial-data marker, and then live changes.
The marker is a barrier, not end-of-stream.

Every write in a bucket receives one monotonically increasing bucket revision.
Revisions are global to the bucket, not consecutive per key or per filtered
watch.  ``Create`` succeeds only for an absent key and ``Update`` succeeds only
when the latest key revision matches the caller's expectation.  ``Delete``
leaves a deletion marker while retaining history according to bucket policy;
``Purge`` removes prior revisions and leaves a purge marker.  A watcher must
observe both operations, including expiry markers, because every one removes a
key from the materialized view.

KV watch is not a durable event subscription.  The initial snapshot normally
contains only the latest operation per key, and a consumer recreated after a
gap may legitimately skip intermediate writes.  This is exactly the desired
contract for a cache and the wrong contract for an audit trail.  This RFC
therefore keeps ``NatsKvWatchService`` separate from
``NatsJetStreamConsumerService`` and never exposes a switch which changes one
into the other.

TSD cache semantics
~~~~~~~~~~~~~~~~~~~

``publish_tsd<K,V>`` maps one logical cache to a reserved key namespace in an
existing bucket.  The cache protocol has four record classes:

``head``
   The committed generation, protocol version, key and child schema
   descriptors, codec identity, publisher identity, staged-image entry count,
   and staged-image digest.  Updating ``head`` is the atomic visibility point
   for a newly staged image.  The count/digest validates that transition; it is
   not a checksum of later live state.

``generation data``
   One record per live TSD key beneath the generation.  The physical KV key
   contains a fixed SHA-256 digest of the canonical encoded TSD key.  The value
   contains the complete encoded key, the complete **current image** of the TSD
   child, protocol metadata, and a checksum.  Including the key makes the
   digest collision detectable rather than silently aliasing two values.

``generation tombstone``
   A KV delete or purge marker for a data key.  Both remove the corresponding
   child from a subscribed TSD.  Publisher cleanup of an obsolete generation
   is distinguishable by generation and cannot delete a child from the current
   one.

``generation status``
   Publisher state, generation, monotonically increasing publisher sequence,
   heartbeat time, and optional source-quality metadata.  The publisher
   refreshes it at a configured finite interval and writes ``Stopped`` during
   orderly shutdown when possible.  Missing heartbeats make a subscriber
   ``Stale`` without deleting its last coherent TSD.  This is freshness
   evidence, not a lease or permission to take over.

The exact reserved spelling is versioned, but the shape is
``<prefix>.head``, ``<prefix>.g.<generation>.status``, and
``<prefix>.g.<generation>.d.<digest>``.  User data may not write beneath a
cache's reserved prefix.  Bucket and prefix are deployment identities; neither
is taken from a ticking time series.

The staged-image digest is SHA-256 over the lexicographically ordered sequence
of ``(physical data key, record checksum)`` pairs with length-delimited fields
and a protocol-version domain separator.  It is reproducible without decoding
child values and independent of KV delivery batching.  Hashes never replace
descriptor equality or the embedded-key comparison.

The initial publisher sequence is:

1. bind the key and child codecs to the resolved ``TSD<K,V>`` schema;
2. capture one owned coherent current TSD image on the graph thread and
   allocate a fresh, uncommitted generation;
3. write one full child image for every key live in that capture;
4. conflate source changes arriving during staging into the bounded dirty set;
5. wait for every attributed staged-image mutation result;
6. compare-and-swap ``head`` to the new generation; and
7. publish current-generation status and release the dirty states as live
   per-key mutations.

An attached consumer may buffer one bounded candidate uncommitted generation.
On the ``head`` mutation, an uninterrupted watch has already observed every
preceding staged write.  If the candidate matches the head's staged-image
count and digest, it calls ``apply_current_value`` once.  If the candidate was
not retained, is incomplete, or fails validation, the consumer keeps its old
coherent TSD, enters ``Recovering``, and opens a fresh snapshot watch for the
now-committed generation.  A newly attached consumer similarly buffers the KV
initial snapshot until the end-of-initial-data marker, selects the generation
named by the snapshot's ``head``, and performs the one-tick replacement.  The
watch barrier, rather than the original staged-image digest, defines
completeness after live updates have changed that generation.  Old generations
are ignored and may be purged only after the new head commit.  This protocol
means that startup, publisher replacement, and recovery cannot expose a
half-image or leave keys from a prior publisher as ghosts.

After commit, a modified TSD child writes its complete current child image and
a removed child deletes its generation data key.  Writing an image rather than
a nested child delta is mandatory: a late watcher receives only the latest KV
value and must be able to construct the child without replaying its history.
Pending writes are conflated per TSD key, using the newest full image or
tombstone.  This is structural, state-aware conflation; raw serialized deltas
are never overwritten on the assumption that "last bytes win".

One source tick may modify several TSD keys.  NATS KV supplies no multi-key
transaction, so those mutations may appear on different graph cycles and a
consumer may briefly observe a mixed state.  The cache guarantees eventual
per-key convergence after acknowledged writes, not preservation of source tick
boundaries.  A graph requiring atomic multi-key live ticks or every
intermediate update must publish whole-TSD ``Image``/``Delta`` frames through
JetStream under RFC 0017 instead.

One publisher is authoritative for a cache generation.  Generation-qualified
data keys fence a replaced publisher: late writes from it remain beneath its
old generation and are ignored once ``head`` changes.  ``head`` is committed
with compare-and-swap, concurrent commit conflict is fatal by default, and a
publisher watches the head it owns.  A deliberate takeover is an explicit
wiring-time policy; this RFC does not claim that KV CAS is a distributed lease.

Ownership boundary
------------------

``hgraph-nats`` owns:

* all NATS message, header, subscription, request, result, JetStream metadata,
  acknowledgement-token, KV entry, KV mutation, TSD cache protocol,
  configuration, and event types;
* NATS C client discovery, linking, options, callbacks, and extension-owned
  C++ RAII wrappers;
* the connection, JetStream context, subscription registry, request inbox
  multiplexer, pull-consumer and KV watcher owners, timers, and shutdown
  protocol;
* Core, JetStream, and KV service descriptors, the multi-interface
  implementation, graph nodes, typed TSD cache protocol, and wiring helpers;
* the fake transport, broker integration tests, examples, and performance
  evidence; and
* the cache envelope/key framing and optional Python authoring bridge.

hg_cpp owns only the existing facilities consumed by the extension:

* native graph and node authoring;
* subscription, request/reply, reference services, and transport planning;
* bounded real-time push-source policies and sender lifetime;
* named extension scalar and Python-class registration;
* the installed CMake SDK and optional Python bridge boundary; and
* RFC 0017's schema descriptors and canonical binary value codec, once that
  prerequisite is accepted and implemented.

Core must not include NATS headers, link the NATS C library, import
``hgraph_nats`` during a normal ``hgraph`` import, or declare a package
dependency on the extension.  A native-only hgraph build remains independent
of NATS and Python.

The first-party extension lives under ``extensions/nats`` with its own CMake
package, Python distribution, version, changelog, tests, examples, and release
artifact.  Co-location allows one repository change to validate the core SDK
and extension together without reversing their dependency direction.  There
is no existing hgraph NATS API to migrate or retain through a core compatibility
shim.

No new generic messaging types are added to core.  NATS Object Store, bucket
or stream administration, consumer administration, NATS microservices,
scatter/gather, cache-cohort routing, and cross-region cache replication remain
outside the initial extension API.  KV watch and TSD cache are included here
as cached-state contracts and remain distinct from the event interfaces.

The raw KV services move opaque bytes and can be implemented independently.
The typed TSD helpers require RFC 0017's canonical schema descriptors and
binary value converter.  They use the converter for the TSD key and each
child's ``value_schema`` but do not reuse RFC 0017's live stream envelope:
NATS KV revisions and the extension's generation/head records provide the
cache framing.  RFC 0030's persistence codec registry is not a dependency; a
store-selected codec must not silently determine a network cache format.

Public value contract
---------------------

The semantic shapes in this section are normative while exact field spelling
remains reviewable during the Proposed phase.  Public compound scalars use
named ``Bundle`` schemas, collections of time-series fields use named ``TSB``
schemas, and repeated scalar collections use immutable
``HomogeneousTuple<T>`` values.  Python classes register those same native
schemas rather than defining a parallel dataclass representation.

Core message types
~~~~~~~~~~~~~~~~~~

``NatsHeader``
   One exact-case header name and its ordered tuple of string values.  NATS
   permits more than one value for a name.  Header-name ordering is not a
   public guarantee, so callers must address a header by name rather than by
   tuple position.  Empty values remain distinct from a missing header.

``NatsMessage``
   The concrete subject, optional reply subject, headers, and payload bytes.
   An empty payload remains a valid zero-length byte string.  Core NATS has no
   server message timestamp, sequence, or delivery cursor, so the extension
   does not invent one.

``NatsOutboundMessage``
   Payload bytes, headers, optional reply subject, and an optional user token.
   A normal publish helper leaves the reply subject unset.  A responder can
   publish to the received message's reply subject through the same Core
   publish service.

``NatsSubscriptionKey``
   Subject filter, optional queue group, and optional sharing identity.  The
   key is immutable and hashable.  Equal keys within one service path share
   one NATS subscription and one graph publication.  A non-empty sharing
   identity requests an independent subscription even when subject and queue
   group match, which is necessary when one process intentionally contributes
   more than one queue-group member.

``NatsSubscriptionState``
   ``Starting``, ``Active``, ``Draining``, ``Stopped``, or ``Failed``.

``NatsCoreSubscriptionOutput``
   A named time-series bundle containing
   ``message: TS[Shared[NatsMessage]]`` and
   ``state: TS[NatsSubscriptionState]``.  A message tick is immutable and fully
   owned by hgraph.  State may tick independently.

``NatsPublishRequest``
   A named time-series bundle containing ``subject: TS[Str]`` and
   ``message: TS[NatsOutboundMessage]``.  Static-subject wiring supplies a
   constant subject time series rather than copying the subject into every
   message scalar.

``NatsCorePublishReport``
   User token, service-assigned sequence, subject, status, NATS status code,
   and diagnostic message.  Status is ``Accepted`` or ``Rejected``.
   ``Accepted`` means accepted by the local client connection or reconnect
   buffer.  It never means that a subscriber existed or processed the value.

``NatsRequest``
   A named time-series bundle containing subject, outbound message, and
   timeout.  The subject must be concrete.  The timeout is measured from the
   task's publication of the request, not from the graph tick which first
   requested it.

``NatsRequestResult``
   A named time-series bundle containing status, optional response message,
   user token, service-assigned generation, and diagnostics.  Status is one
   of ``Response``, ``NoResponders``, ``Timeout``, ``Cancelled``, ``Rejected``,
   or ``TransportError``.  Only ``Response`` carries a message.

JetStream types
~~~~~~~~~~~~~~~

``NatsJetStreamConsumerKey``
   Stream name, durable consumer name or explicit ephemeral mode, subject
   filters, starting delivery policy, optional start sequence/time, replay
   policy, pull limits, acknowledgement wait, maximum deliveries,
   ``MaxAckPending``, and sharing identity.  The initial contract either binds
   an existing durable consumer without mutating it or creates a graph-owned
   ephemeral pull consumer.  A bound consumer's server configuration is
   validated against the requested semantic fields before delivery starts.

``NatsJetStreamMetadata``
   Stream, consumer, optional domain, stream sequence, consumer sequence,
   stored timestamp, delivery count, and pending count.  These fields are
   broker metadata; they do not rewrite live hgraph evaluation time.

``NatsJetStreamAckToken``
   An extension-issued token plus service path identity, consumer identity,
   consumer generation, stream sequence, consumer sequence, and delivery
   count.  It is safe to retain as immutable graph data, but only the owning
   live service generation can act on it.  Constructed, stale, duplicate-final,
   or cross-path tokens are rejected and reported.

``NatsJetStreamConsumerOutput``
   A named time-series bundle containing
   ``message: TS[Shared[NatsMessage]]``,
   ``metadata: TS[NatsJetStreamMetadata]``,
   ``ack_token: TS[NatsJetStreamAckToken]``, and
   ``state: TS[NatsSubscriptionState]``.  Message, metadata, and token tick
   together.  State may tick independently.

``NatsJetStreamPublishOptions``
   Optional stable message id, expected stream, expected last stream sequence,
   expected last subject sequence, expected last message id, and timeout.
   These are factual JetStream preconditions, not an ``ExactlyOnce`` switch.

``NatsJetStreamPublishRequest``
   Subject, outbound message, and publish options as one named time-series
   bundle.

``NatsJetStreamPublishReport``
   User token, service sequence, status, stream, sequence, domain, duplicate
   flag, NATS and JetStream error codes, retryability, and diagnostics.
   ``Acknowledged`` means JetStream stored or de-duplicated the publication as
   described by the report.  It does not mean any consumer processed it.

``NatsJetStreamAckRequest``
   Ack token, disposition (``Ack``, ``Nak``, ``Term``, or ``InProgress``),
   optional ``Nak`` delay, and confirmation policy.  ``Ack``, ``Nak``, and
   ``Term`` are final for the token; ``InProgress`` retains it.

``NatsJetStreamAckReport``
   Ack token identity, disposition, status (``Sent``, ``Confirmed``,
   ``Rejected``, or ``TransportError``), error codes, and diagnostics.  A
   confirmed report is only transport confirmation from JetStream.

Key/Value and TSD cache types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``NatsKvWatchKey``
   Bucket, one or more exact/wildcard key filters, watch mode, optional resume
   revision, include-history, ignore-deletes, metadata-only,
   heartbeat/timeout limits, and sharing identity.  Watch mode is one of
   ``SnapshotThenUpdates``, ``UpdatesOnly``, or ``Resume``.  The TSD cache
   helper always uses ``SnapshotThenUpdates`` with latest-only history, values
   present, and deletes included for initial attach and recovery; it never
   resumes when a complete image is required.

``NatsKvEntry``
   Bucket, concrete key, value bytes, bucket revision, creation timestamp,
   remaining initial-snapshot delta count when available, and operation
   (``Put``, ``Delete``, or ``Purge``).  Delete and purge entries carry no
   application value.  All strings and bytes are owned before leaving the NATS
   task.

``NatsKvWatchOutput``
   A named time-series bundle containing
   ``entry: TS[Shared[NatsKvEntry]]``,
   ``state: TS[NatsKvWatchState]``, and
   ``snapshot_revision: TS[Int]``.  ``NatsKvWatchState`` distinguishes
   ``Starting``, ``Snapshot``, ``Live``, ``Recovering``, ``Stopped``, and
   ``Failed``.  The ``Snapshot`` to ``Live`` transition is the public
   end-of-initial-data barrier; an empty bucket still emits it.  The revision
   is the greatest initial entry revision observed, not a transaction id for
   the whole bucket.

``NatsKvMutationRequest``
   Bucket, concrete key, operation (``Put``, ``Create``, ``Update``,
   ``Delete``, or ``Purge``), value bytes where applicable, optional expected
   revision, optional create-only per-key TTL, timeout, and user token.
   ``Update`` requires an expected revision.  Bucket creation and configuration
   are deliberately absent.

``NatsKvMutationReport``
   User token, service generation, bucket, key, operation, status, assigned
   revision when one exists, current/conflicting revision when reported, NATS
   and JetStream codes, retryability, and diagnostics.  Status distinguishes
   ``Applied``, ``Conflict``, ``NotFound``, ``Rejected``, ``Timeout``, and
   ``TransportError``.

``NatsTsdCacheSpec``
   Immutable bucket, cache prefix, codec/protocol version, publisher identity,
   takeover policy, image and pending-dirty bounds, failure policy, and
   obsolete-generation retention, publisher heartbeat interval, and subscriber
   freshness timeout.  The timeout must exceed the interval by a validated
   margin.  The resolved ``K`` and ``V`` schemas are supplied by wiring and are
   not repeated as user strings.

``NatsTsdCacheState``
   ``Warming``, ``Live``, ``Recovering``, ``Stale``, ``Failed``, or
   ``Stopped``.  Connectivity is reported separately through ``NatsEvent``;
   for example, a disconnected subscriber or one missing current-generation
   heartbeats remains ``Stale`` with its last coherent value rather than
   becoming invalid.

``NatsTsdCacheOutput<K,V>``
   A named time-series bundle containing
   ``value: TSD<K,V>``, ``state: TS[NatsTsdCacheState]``,
   ``generation: TS[Str]``, and ``revision: TS[Int]``.  ``value`` remains
   invalid while the first image is incomplete.  It becomes valid only when
   the complete image is applied, remains at the last coherent state during
   recovery, and is atomically replaced by the recovered image.

``NatsTsdPublisherOutput``
   Publisher state, committed generation, head revision, greatest acknowledged
   data revision, pending-dirty key count, last successful synchronization
   time, last status revision, and an optional typed error.  ``Live`` means the
   initial image head is committed, its heartbeat is current, and every
   accepted source change is either acknowledged or held in the bounded
   per-key conflation set; it does not mean every remote consumer has observed
   it.

Events
~~~~~~

``NatsEvent``
   Evaluation-independent event data: severity, category, connection state,
   service path, optional subject/stream/consumer identity, NATS and JetStream
   status codes, retriable/fatal flags, dropped message/byte counters, and a
   redacted diagnostic.  Categories cover connecting, connected,
   disconnected, reconnected, lame-duck, closed, async error, slow consumer,
   request, publish, JetStream consumer, acknowledgement, KV watch, KV
   mutation, TSD image/recovery, cache conflict, and shutdown.

Credentials, tokens, seeds, passwords, and raw option values never appear in
messages, reports, events, exception text, debugger views, or ``repr`` output.

Configuration contract
----------------------

Configuration is immutable graph-time scalar data.  Builders may construct it
conveniently, but the built value is not a live connection manager.

``NatsServiceConfig``
   Complete configuration for one service path: connection, authentication,
   TLS, reconnect, ingress bounds, failure policy, Core publish limits,
   JetStream and KV defaults, shutdown drain, and observability labels.
   Clients of one path share this configuration.  A materially different
   connection or policy requires another path.

``NatsConnectionConfig``
   Server URL tuple, connection name, optional inbox prefix, echo policy,
   ping/timeout settings, reconnect attempts and backoff, reconnect buffer
   bytes, and TLS/authentication references.  The extension registers all
   lifecycle and async-error callbacks before connecting.

``NatsIngressConfig``
   Bounded push-source envelope count, default per-subscription pending message
   and byte limits, and slow-consumer action.  The default slow-consumer action
   is ``StopGraph`` after publishing a typed event.  ``ReportAndContinue`` is
   available only as an explicit acknowledgement that Core messages may be
   lost.

``NatsJetStreamConfig``
   Optional domain/API prefix, publish-ack timeout, maximum outstanding async
   publishes, pull batch count, pull maximum bytes, pull expiry, and shutdown
   timeout.  Per-consumer semantic values remain in
   ``NatsJetStreamConsumerKey`` so equality and resource sharing are exact.

``NatsKvConfig``
   Default watch heartbeat and operation timeout, maximum concurrently active
   watchers, maximum outstanding mutations, maximum initial-image entries and
   bytes, maximum dirty TSD keys and bytes, generation cleanup bound, and
   cache failure policy.  The defaults are finite.  Image bounds apply to the
   graph-side coherent-image buffer as well as transport admission; exceeding
   them produces typed failure rather than exposing a partial TSD.

Authentication supports user/password, token, credentials file, NKey seed
provider, and TLS client certificate references without copying secrets into
ordinary diagnostic values.  Dynamic credential refresh is implemented by an
extension-owned provider whose lifetime is graph scoped.  The extension does
not expose arbitrary callback pointers or NATS C structs in scalar
configuration.

The extension validates configuration before start:

* publish/request subjects are concrete and contain no wildcard token;
* subscription filters follow NATS token and wildcard rules;
* queue groups and durable names satisfy their respective rules;
* counts, bytes, durations, and reconnect buffers are non-negative and have
  internally consistent floors;
* a pull batch cannot exceed configured consumer/server bounds;
* the initial JetStream consumer contract uses explicit acknowledgements; and
* binding an existing durable consumer requires compatible filters,
  acknowledgement policy, delivery policy, and limits;
* KV bucket, key, filter, and reserved cache-prefix syntax follows the NATS KV
  token rules, and arbitrary application keys cannot enter the reserved cache
  namespace;
* ``Resume`` requires a positive revision while
  ``SnapshotThenUpdates`` and ``UpdatesOnly`` reject one; include-history is
  incompatible with ``UpdatesOnly``/``Resume`` rather than being silently
  ignored;
* a TSD cache has finite positive image and dirty-set bounds, an explicit
  takeover policy, a freshness timeout safely above its heartbeat interval,
  and a codec/protocol supported by both schemas; and
* a publisher's key and child schemas are encodable by RFC 0017 and contain no
  process-local ``REF`` value.

At service start, a demanded TSD cache also validates the existing bucket's
capabilities and limits.  The bucket must preserve watch delete/purge markers
and must accept the largest configured encoded child.  The initial typed TSD
protocol rejects bucket-wide and per-key expiry: independently aging data keys
would remove still-valid TSD children.  Freshness is represented by the status
heartbeat while data remains cached.  History depth one is sufficient and
recommended; history replay is not used for correctness.  The extension does
not create or mutate the bucket to make an incompatible deployment appear
valid.  Raw KV users may still use TTL when its deletion semantics are what
they intend.

Public C++ wiring API
---------------------

The authoring surface is ordinary graph wiring.  There is no user-held
``NatsClient`` runtime object:

.. code-block:: cpp

   static void compose(Wiring &w)
   {
       const auto primary = service::path("primary");

       nats::register_service(
           w, primary,
           nats::service_config()
               .servers({"nats://nats-a:4222", "nats://nats-b:4222"})
               .connection_name("risk-graph")
               .max_pending_envelopes(1024)
               .build());

       auto subject = wire<stdlib::const_>(
           w,
           nats::subscription_key()
               .subject("orders.created")
               .build())
           .as<TS<NatsSubscriptionKey>>();

       auto orders = nats::subscribe(w, primary, subject);
       auto decoded = wire<DecodeOrder>(w, orders.field<"message">());

       auto outbound = wire<EncodeResult>(w, decoded);
       auto core_report = nats::publish(
           w, primary,
           nats::publish_request(w, "orders.processed", outbound));
       wire<ObserveCorePublish>(w, core_report);

       auto request = nats::request_value(
           w, "inventory.lookup", wire<EncodeInventoryRequest>(w, decoded),
           Duration{std::chrono::seconds{2}});
       auto response = nats::request(w, primary, request);
       wire<ObserveInventoryResponse>(w, response);

       auto consumer_key = wire<stdlib::const_>(
           w,
           nats::jetstream_consumer_key()
               .stream("ORDERS")
               .durable("risk")
               .filter_subjects({"orders.*"})
               .bind_existing()
               .build())
           .as<TS<NatsJetStreamConsumerKey>>();

       auto durable_orders = nats::consume(w, primary, consumer_key);
       auto processed = wire<ProcessDurableOrder>(
           w, durable_orders.field<"message">());
       auto ack_request = nats::ack_after(
           w, processed, durable_orders.field<"ack_token">());
       auto ack_report = nats::ack(w, primary, ack_request);
       wire<ObserveAck>(w, ack_report);

       const auto prices_cache = nats::tsd_cache_spec()
           .bucket("MARKET_CACHE")
           .prefix("risk.prices")
           .publisher_id("risk-graph")
           .exclusive_publisher()
           .max_image_entries(100'000)
           .max_image_bytes(64 * 1024 * 1024)
           .build();

       auto local_prices = wire<BuildPrices>(w).as<TSD<Str, TS<Float>>>();
       auto publication = nats::publish_tsd(
           w, primary, prices_cache, local_prices);
       wire<ObserveCachePublisher>(w, publication);

       auto remote_prices = nats::subscribe_tsd<Str, TS<Float>>(
           w, primary, prices_cache);
       wire<UseRemotePrices>(w, remote_prices.field<"value">());
       wire<ObserveCacheState>(w, remote_prices.field<"state">());

       wire<ObserveNatsEvent>(w, nats::events(w, primary));
   }

``nats::register_service`` delegates to one
``service::register_services<NatsServiceImpl, ...>`` call with all nine
interfaces, the path, and ``NatsServiceConfig``.  It registers one lazy
materializer and performs no DNS lookup, connection, subscription, or worker
start.  Duplicate registration at a concrete path is a wiring error.

The primary helpers are:

``nats::subscribe(w, path, TS[NatsSubscriptionKey])``
   Wires ``NatsCoreSubscriptionService`` and returns
   ``NatsCoreSubscriptionOutput``.

``nats::publish(w, path, NatsPublishRequest)``
   Wires ``NatsCorePublishService`` and returns
   ``TS[NatsCorePublishReport]``.  Callers may intentionally ignore the report,
   but the runtime still emits rejection events.

``nats::request(w, path, NatsRequest)``
   Wires ``NatsCoreRequestService`` and returns ``NatsRequestResult``.

``nats::consume(w, path, TS[NatsJetStreamConsumerKey])``
   Wires ``NatsJetStreamConsumerService`` and returns
   ``NatsJetStreamConsumerOutput``.

``nats::jetstream_publish(w, path, NatsJetStreamPublishRequest)``
   Wires ``NatsJetStreamPublishService`` and returns
   ``TS[NatsJetStreamPublishReport]``.

``nats::ack(w, path, TS[NatsJetStreamAckRequest])``
   Wires ``NatsJetStreamAckService`` and returns
   ``TS[NatsJetStreamAckReport]``.  ``ack_after`` is composition sugar which
   forwards a token only when the supplied success signal ticks; it is not a
   runtime acknowledgement shortcut.

``nats::watch_kv(w, path, TS[NatsKvWatchKey])``
   Wires ``NatsKvWatchService`` and returns ``NatsKvWatchOutput``.  It is the
   byte-level API for applications which own their own cached-state protocol.

``nats::mutate_kv(w, path, NatsKvMutationRequest)``
   Wires ``NatsKvMutationService`` and returns
   ``TS[NatsKvMutationReport]``.  Network waits and compare-and-swap results
   return asynchronously through the service output.

``nats::publish_tsd<K,V>(w, path, NatsTsdCacheSpec, TSD<K,V>)``
   Returns ``NatsTsdPublisherOutput``.  This is typed graph composition over
   ``mutate_kv`` plus a private head watch; it binds RFC 0017 converters once,
   stages the initial image, commits the head, and thereafter conflates pending
   current child images by key.

``nats::subscribe_tsd<K,V>(w, path, NatsTsdCacheSpec)``
   Returns ``NatsTsdCacheOutput<K,V>``.  It composes ``watch_kv`` with a typed
   graph-side cache node which buffers an image, validates protocol and schema,
   applies one atomic current value, then applies live per-key images and
   structural removals.

``nats::events(w, path)``
   Wires ``NatsEventService`` and returns ``TS[NatsEvent]``.

The service implementation obtains all client inputs through
``service::impl_input``.  Core, JetStream, and KV outbound operations use
distinct sink nodes even though they share the graph-scoped connection.
Request results, JetStream publish acknowledgements, acknowledgement reports,
KV mutation results, KV watch entries, and events enter through the one
transport push source and are published through ``service::impl_output``.
RFC 0014 consequently recognizes those external responses as decoupled and
selects direct transport without a user flag.  A Core publish report is instead
the immediate output of the graph-thread publish sink, so the planner correctly
treats that one interface as causally dependent on its request and applies the
normal one-cycle request relay.

Python API
----------

``hgraph_nats`` mirrors the native service surface.  Registration and helper
calls wire native services; no Python object owns the connection and no Python
event loop or callback thread implements transport semantics:

.. code-block:: python

   from hgraph_nats import (
       NatsCorePublishStatus,
       ack,
       ack_after,
       consume,
       events,
       jetstream_consumer_key,
       publish,
       publish_request,
       publish_tsd,
       register_service,
       service_config,
       subscribe,
       subscribe_tsd,
       subscription_key,
       tsd_cache_spec,
   )

   @graph
   def app() -> TS[NatsCorePublishStatus]:
       register_service(
           "primary",
           service_config(servers=("nats://localhost:4222",)),
       )
       messages = subscribe(
           "primary", subscription_key(subject="orders.created")
       )
       reports = publish(
           "primary", publish_request("orders.received", messages.message)
       )
       return reports.status

   @graph
   def remote_prices(local_prices: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
       cache = tsd_cache_spec(
           bucket="MARKET_CACHE",
           prefix="risk.prices",
           publisher_id="risk-graph",
       )
       publish_tsd("primary", cache, local_prices)
       subscribed = subscribe_tsd(
           "primary", cache, TSD[str, TS[float]]
       )
       return subscribed.value

Python values are registrations of the same named native schemas.  Native byte
transport and the RFC 0017 TSD codec acquire no GIL.  A Python graph callable
runs on the graph thread under the GIL after the native value has crossed the
push boundary; a Python-defined cache codec is not part of the initial wire
contract.  The Python wheel does not depend on ``nats.py`` and does not carry a
second connection implementation.

Runtime architecture
--------------------

Each materialized service implementation owns one graph-local resource.  It
contains no Python object on the native path and no process-global mutable
state.  Two graph engines may run concurrently with separate service paths and
connections.

The resource owns:

* one NATS C connection and one JetStream context;
* Core subscription owners keyed by the complete semantic subscription key;
* JetStream pull-consumer owners keyed by the complete consumer key;
* KV store handles and watcher owners keyed by bucket, filters, options, and
  sharing identity;
* one request inbox prefix, correlation registry, and timeout scheduler;
* outstanding Core, request, JetStream publish, ack, and KV mutation
  generations keyed by stable hgraph client id;
* the framework-created bounded burst sender for all external ingress;
* callback-completion and shutdown state; and
* data-only counters and inspection views.

NATS callbacks copy the complete subject, reply subject, header values,
payload, status, and JetStream metadata into an owned transport envelope before
returning.  No ``natsMsg*``, ``jsPubAck*``, callback closure, graph port,
``STATE``, node, scheduler, or executor escapes its valid lifetime.

The graph-thread Core publish sink calls only the NATS client's non-network-
waiting publish path.  It returns an immediate local report.  Reconnect-buffer
and maximum-payload rejection are visible.  The sink never flushes or waits on
a round trip.  JetStream publish uses the bounded asynchronous publish API;
the graph thread observes immediate admission failure, while the server
acknowledgement or later error returns through the push sender.

Core requests use one connection-level inbox multiplexer rather than one
blocking worker per request.  Each request carries the hgraph client id and a
monotonic generation.  The first reply, no-responder status, timeout, explicit
replacement, or client removal wins atomically and retires that generation.
Late replies cannot revive a replaced client.

JetStream consumers use finite pull requests.  One owner serializes pull,
consumer inspection, and teardown for each shared consumer binding.  Pulled
messages are manually acknowledged and sent to the graph with
``send_blocking``.  When hgraph capacity is full, the owner stops fetching;
the server's ``MaxAckPending`` and pull limits remain the authoritative
in-flight bounds.  The extension never auto-acks when a message is fetched,
copied, queued, dequeued, or published into a time series.

Acknowledgement operations execute off the graph evaluation thread when a
server-confirmed round trip is requested.  The task retains only the bounded
protocol state required by live unacknowledged tokens.  A final ack removes
that state after send/confirmation according to policy; ``InProgress`` keeps
it.  Graph teardown leaves unacknowledged messages for normal JetStream
redelivery.

KV watchers are task-owned blocking readers over ``kvWatcher_Next``.  Each
entry is copied into an owned envelope and admitted with ``send_blocking``;
the null end-of-initial-data entry becomes a first-class lifecycle envelope.
``kvWatcher_Stop`` cancels a blocked read during teardown.  KV mutation sinks
enqueue finite command state, and a service task performs any network wait and
returns the attributed result through the same push sender.  Neither watcher
nor mutation task writes a graph output directly.

The typed TSD publisher and subscriber are graph-side owners layered on those
services.  They bind their RFC 0017 key and child converters during start and
retain reusable buffers.  The publisher holds a bounded map from TSD key to
latest pending full image/tombstone plus bounded in-flight mutation metadata.
The subscriber holds at most one bounded candidate image and one committed TSD
state.  Encoding, decoding, schema validation, head state transitions,
whole-TSD ``apply_current_value``, per-child ``apply_current_value``, and TSD
structural removal all run on the graph thread; NATS callbacks remain
byte-only.

The publisher schedules status heartbeats from a graph-side node and sends them
through the same asynchronous KV mutation lane; no timer callback mutates the
graph.  The subscriber measures freshness against its live engine clock and
the arrival of status for the committed generation.  A late old-generation
heartbeat is ignored.  Wall-clock fields in the status record are diagnostic;
local receipt and the configured timeout decide ``Stale`` so clock skew cannot
silently certify fresh data.

Any ambiguous publisher outcome, lost watch continuity, malformed record,
schema mismatch, digest/key mismatch, or image-bound breach transitions the
cache through typed failure policy.  The normal recoverable path opens a fresh
snapshot watch, retains the last committed local TSD as ``Stale`` or
``Recovering``, and replaces it only after a new complete image.  It never
continues applying a stream whose materialized basis is uncertain.

Ingress projection and ordering
-------------------------------

One internal ``NatsTransportEnvelope`` distinguishes:

* Core subscription message and lifecycle;
* Core request result;
* JetStream consumer message and lifecycle;
* JetStream publish result;
* JetStream acknowledgement result; and
* KV entry, snapshot barrier, watcher lifecycle, and mutation result; and
* service event.

The standard burst source admits one envelope per sender call and delivers all
currently pending envelopes as an ordered tuple.  One NATS-specific emitter
walks that tuple plus any retained tail and writes a collision-free prefix to
the structural service lanes.  Different subscription keys and request ids
may update in one engine cycle.  A second value for an already-written key, a
second scalar event, or a lifecycle barrier remains at the front for the next
``MIN_TD`` cycle; later envelopes do not overtake it.  Standard projection and
mapped ``emit`` nodes produce the public outputs.

The extension promises:

* the order in which one Core subscription's callback delivers messages is
  retained at that subscription output;
* the order in which one JetStream consumer yields messages is retained,
  subject to visible broker redelivery;
* the order in which one KV watcher yields initial entries, its barrier, and
  live entries is retained;
* request, publish, acknowledgement, and KV mutation results are correlated to
  their stable hgraph client id and generation;
* no conflation of message, result, or event values; and
* no invented total order across independent subjects, subscriptions,
  consumers, or NATS client callbacks.

Live message evaluation time is the graph cycle which emits it.  A JetStream
or KV stored timestamp is metadata and does not move live graph time backward
or forward.  Per-key conflation exists only inside ``publish_tsd``'s declared
cached-state protocol; it never changes raw ``watch_kv`` delivery.

Capacity and failure policy
---------------------------

The standard bounded push source is the only cross-thread ingress queue owned
by the extension.  It owns its mutex, condition variable, receiver lifetime,
executor notification, and shutdown wake-up.  NATS code does not reproduce
those mechanisms.

Core callbacks use blocking sender admission.  This transfers pressure into
the NATS client's configured per-subscription pending message/byte buffers.
Those buffers are also finite.  If the client reports a slow consumer or
dropped messages, the extension emits one typed event with cumulative counters
and applies the configured graph-thread policy.  The default is to stop rather
than continue a stream with silent gaps.  ``ReportAndContinue`` remains valid
for explicitly lossy telemetry.

JetStream pull owners also use blocking admission, but stop issuing pulls when
blocked.  Pull batch count/bytes, hgraph envelope capacity, client pending
limits, ``MaxAckPending``, and the async publish pending limit jointly define
the live memory bound.  Configuration and inspection expose each layer
separately rather than one misleading aggregate count.

KV watch owners block at the same admission boundary.  A typed TSD subscriber
additionally bounds the candidate image by entry count and encoded/decoded
bytes.  A typed publisher bounds both outstanding KV requests and the
per-key dirty set.  Repeated changes to one dirty key replace its pending full
image, but distinct dirty keys consume distinct capacity.  When a bound is
exceeded the cache becomes ``Recovering`` or ``Failed`` according to its
declared policy; it does not drop an entry and continue claiming ``Live``.

On reconnect, watcher error, ordered-consumer reset, or any other continuity
ambiguity, ``subscribe_tsd`` re-images rather than relying on numeric revision
adjacency.  Gaps are normal for a filtered watch because unrelated bucket keys
consume revisions.  ``ResumeFromRevision`` remains available to the raw KV API
for an application which persists its own state, but the TSD helper favors a
fresh bounded image as the correctness primitive.

No NATS worker or callback calls ``EvaluationEngineApi``, schedules a node,
mutates a time series, or stops a graph.  A fatal envelope enters through the
push sender, and a normal graph-thread node applies ``Report`` or
``StopGraph``.

Protocol confirmation
---------------------

The following events are intentionally different:

* Core publish ``Accepted``: the local client accepted or buffered bytes;
* NATS flush completion: a server round trip observed previous writes, not
  exposed as a per-message success in the initial API;
* JetStream publish ``Acknowledged``: the stream accepted or de-duplicated the
  message;
* KV mutation ``Applied``: the server assigned the reported bucket revision;
* TSD publisher ``Live``: its head is committed and every source key is
  acknowledged or represented by one bounded pending current state;
* TSD subscriber ``Live``: one complete generation is materialized and the
  current watch remains continuous with timely status heartbeats;
* graph delivery: hgraph emitted a received message; and
* JetStream consumer ``Confirmed`` ack: the server confirmed the disposition
  sent after the graph explicitly supplied the token.

None proves an arbitrary downstream side effect.  The application chooses its
processing boundary by wiring the ack token after that operation.  Automatic
ack on callback return, sender admission, or push-source dequeue is forbidden.

``Ack`` does not imply exactly-once processing.  A worker may complete a side
effect and fail before the server sees its ack, causing redelivery.  Users must
make side effects idempotent or supply a separate transactional/checkpoint
protocol.  ``Nats-Msg-Id`` de-duplicates qualifying JetStream publications
within server policy but does not make other side effects atomic.

Lifecycle and teardown
----------------------

Start order is:

1. initialize graph-side projection, result, and command nodes;
2. start the transport push source and receive its framework-owned sender;
3. construct NATS options and install lifecycle, async-error, and publish-ack
   callbacks;
4. create the graph-scoped connection and JetStream context;
5. start the request inbox subscription and demanded Core subscriptions;
6. bind/start demanded JetStream pull-consumer and raw KV watcher owners;
7. bind TSD cache codecs and start their snapshot/head watches; and
8. publish ``Active``/``Snapshot``/``Warming`` states, with a TSD cache becoming
   ``Live`` only after its image protocol completes.

Partial start rolls back every completed step in reverse order using the
existing scope guards.  A failed connection or consumer bind becomes typed
data before the configured graph failure policy runs.

Stop order is:

1. atomically reject new graph-side publish, request, pull, ack, KV mutation,
   and TSD cache work;
2. let push-source stop close its receiver before invoking the resource stop
   hook, so blocked sender calls return ``false``;
3. cancel request timers, prevent new pull or KV requests, stop KV watchers,
   and wake task-owned waits;
4. unsubscribe or drain Core subscriptions and stop JetStream/KV owners;
5. wait for all extension and NATS callback completions;
6. apply the configured finite outbound and connection drain timeout;
7. destroy subscriptions, JetStream context, and connection in dependency
   order; and
8. release the graph-scoped resource and retained protocol state.

The resource destructor provides a ``noexcept`` emergency close when orderly
graph stop was skipped.  Normal shutdown timeout or drain failure is reported
before destruction.  Retained senders are inert after teardown.

Removing one subscription or KV watch stops only that client reference.  An
equal key continues while another client uses it.  Removing the last reference
waits for the callback completion, pull owner stop, or ``kvWatcher_Stop``
completion before publishing the ``Stopped`` state.  A later equal key receives
a new service generation, so late work from the former owner cannot enter it.
Stopping a TSD publisher does not purge its committed generation or head: the
point of the cache is to remain available while the producer is absent.

Simulation
----------

The initial live implementation requires a real-time root graph.  Registration
in simulation fails at wiring with a diagnostic naming the service path and
live interface.  A fake callback transport used by lifecycle tests follows the
same rule.

JetStream durability and KV cached state are not sufficient reasons to read a
changing remote system during simulation.  A future replay facility must first
capture an explicit finite stream/consumer or KV-image boundary into recorded
hgraph data, then use a scheduled or pull-source node to emit that immutable
snapshot at declared timestamps.  It must not start a NATS worker, acknowledge
a live consumer, or keep a live KV watch.

Packaging and ABI
-----------------

The extension exports a native CMake package and target, public headers under
``include/hgraph/nats``, and an optional ABI3 Python module/package named
``hgraph_nats``.  A native installation can build and use the extension without
Python or nanobind.

The official NATS C API remains behind an extension-owned RAII layer.  No
``natsConnection``, ``natsSubscription``, ``natsMsg``, ``jsCtx``, or other NATS
C type appears in a public hgraph value, service signature, installed debugger
view, or Python capsule.

CMake prefers a compatible installed NATS C package and may fetch a pinned
audited release when explicitly enabled.  The fetched build disables the
deprecated NATS Streaming/STAN API, examples, tests, and experimental APIs.
TLS support and its OpenSSL linkage are packaged consistently with the Web and
Kafka extension wheel policies on macOS, Linux, and Windows.  The exact pinned
version and CMake target normalization are implementation details recorded in
the extension build and changelog.

The source distribution includes CMake files, public headers, native sources,
Python sources, tests, tools, installed-package fixture, and
``python/examples``.  The wheel contains only ``hgraph_nats`` and never writes
files into the core ``hgraph`` package.

Performance and memory
----------------------

The native byte path performs no GIL acquisition.  Inbound callbacks make one
fully owned copy from NATS-managed message storage into an immutable hgraph
message value; all graph clients sharing the subscription then share that
value.  Projection does not recopy the payload per client.  Outbound publish
constructs one NATS message from the graph value and transfers ownership to the
client only where the NATS API documents that transfer.

The implementation records benchmark and memory baselines for:

* Core exact-subject and wildcard fan-out with one and many graph clients;
* Core queue-group consumption and Core request latency;
* JetStream pull throughput under batch, byte, and ``MaxAckPending`` bounds;
* asynchronous JetStream publish acknowledgement throughput;
* explicit and confirmed acknowledgement latency;
* raw KV snapshot/watch and mutation throughput;
* TSD initial image latency and peak memory versus entry count and child size;
* TSD live per-key update/remove throughput, dirty-key conflation, and
  multi-subscriber fan-out;
* publisher generation commit, subscriber re-image, and obsolete-generation
  cleanup cost;
* status-heartbeat overhead and freshness-transition latency;
* repeated-key burst unrolling; and
* reconnect, slow-consumer, and shutdown-drain behavior.

No public guarantee is made from a benchmark result.  The normative properties
are bounded memory, visible refusal/failure, preserved per-stream order, and no
graph-thread network wait.

Compatibility and versioning
----------------------------

There is no released hgraph NATS API, so the initial extension has no legacy
decorator or import-path compatibility obligation.  Core, JetStream, KV, and
cache protocol types are versioned as extension-owned named schemas.  Adding
optional fields follows the repository's schema-compatibility rules; changing
delivery meaning, acknowledgement behavior, token identity, cache key layout,
head semantics, or service shape requires an RFC amendment or successor.

The NATS server and client evolve independently.  The extension publishes a
tested minimum server/client matrix.  Optional server capabilities are detected
at start and either exposed as typed capability state or rejected when the
requested configuration requires them.  The extension never silently weakens
no-responder handling, header support, JetStream preconditions, or confirmed
acknowledgement.  A TSD subscriber rejects an unsupported cache protocol or
non-equal key/child schema descriptor before decoding any application value.

Alternatives considered
-----------------------

Use ``nats.py`` in a Python adaptor
   Rejected.  It would make Python the transport owner, acquire the GIL on the
   hot path, and provide no first-class native graph API.

Add a generic message-bus extension first
   Rejected.  Core NATS, JetStream, and Kafka do not share one honest delivery,
   cursor, acknowledgement, or overflow contract.  They can reuse hgraph
   services without erasing broker semantics.

Reuse Kafka record and cursor types
   Rejected.  NATS subjects, reply subjects, Core at-most-once behavior,
   JetStream consumer sequences, and ack dispositions are not Kafka topics,
   partitions, offsets, or commits.

Expose the whole NATS API in the first release
   Rejected.  Stream/bucket administration, Object Store, microservices,
   scatter/gather, and ordered/push consumer profiles need separate semantics
   and evidence.  The first release covers the reusable event and cached-state
   data planes.

Use JetStream push consumers
   Rejected for the initial contract.  Pull count, bytes, expiry, and
   ``MaxAckPending`` compose directly with bounded hgraph admission.  Push
   consumers introduce another flow-control and heartbeat state machine before
   that path has implementation evidence.

Auto-ack when the callback or push sender succeeds
   Rejected.  Sender admission is buffering and dequeue only begins graph
   processing.  Neither proves application success.

Return no Core publish result
   Rejected.  Maximum-payload and reconnect-buffer failures must be attributable
   to the publisher.  The result is explicitly local acceptance, not delivery.

Run synchronous network calls on the graph thread
   Rejected.  Broker round trips and timeout waits belong to the task and
   return through the push source.

Store one encoded whole TSD in one KV key
   Rejected as the primary contract.  It preserves tick atomicity but rewrites
   and copies the complete TSD for every changed key, defeats KV's per-key
   materialization and watch filtering, and is bounded by one NATS value.  It
   remains an application-level choice through the raw KV API for small maps
   whose atomic replacement matters more than incremental cost.

Store child deltas as KV values
   Rejected.  A new watcher normally receives only the latest value per key.
   The last nested delta is not a child image and cannot reconstruct current
   state without replaying history, so it would make late subscribers wrong.

Publish directly into one permanent generation
   Rejected.  A restarted publisher cannot distinguish its new initial state
   from stale keys left by the prior run, and deleting then repopulating exposes
   a partially empty cache.  Staging a new generation and committing ``head``
   provides one explicit replacement boundary.

Expose snapshot entries as a TSD while the image is arriving
   Rejected for ``subscribe_tsd``.  It would make a partial remote dictionary
   indistinguishable from a complete one.  The raw ``watch_kv`` API remains
   streaming for users who want progressive warm-up and explicitly observe its
   ``Snapshot`` state.

Claim atomic remote TSD ticks over per-key KV
   Rejected.  NATS KV has no multi-key transaction.  The proposed contract
   makes initial/recovery images atomic and live updates convergent; JetStream
   framing is the event/tick-accurate alternative.

Use a process-global connection manager
   Rejected.  Configuration and lifetime belong to one graph service path, and
   concurrent graph engines must not share mutable transport state implicitly.

Advertise exactly-once mode
   Rejected.  JetStream de-duplication and confirmed acknowledgements do not
   coordinate arbitrary graph state or side effects across failure and restore.

Open questions for review
-------------------------

* Whether confirmed acknowledgement is required in the first implementation
  milestone or may follow the initial ``Sent`` report.  The public distinction
  is reserved either way; ``Sent`` must never be presented as ``Confirmed``.
* Whether the first implementation should expose an explicit path-wide flush
  barrier.  It is intentionally absent from per-message Core publish reports
  because flush cannot prove subscriber delivery.
* Which supported NATS C release becomes the initial minimum and fetched pin.
  The selection must support the required Core headers, no-responder status,
  JetStream async publish callbacks, pull bounds, confirmed ack behavior, KV
  watch barriers, resume revisions, CAS, and watcher cancellation on all
  supported platforms.
* Whether the first typed-cache milestone supports every non-``REF`` child
  time-series schema accepted by RFC 0017, or begins with
  ``TSD<K,TS<V>>`` while retaining the generic public protocol.  The wire must
  not acquire a scalar-only shape which prevents later nested children.
* Whether obsolete generations are purged eagerly by the successful publisher
  or retained for an operator-configured grace period and cleaned by a separate
  maintenance graph.  Cleanup is not allowed to delay head commit or invalidate
  the current generation.

Acceptance criteria
-------------------

Public C++ and extension boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A separately built, Python-free installed-SDK consumer wires Core subscribe,
  publish and request; JetStream consume, publish and ack; raw KV watch and
  mutate; typed TSD publish and subscribe; and events.
* Core configures, builds, and tests without NATS headers, the NATS C library,
  Python, or ``hgraph-nats``.
* No NATS C type or Python object crosses the public native boundary.
* One service implementation materializes per demanded path and configuration;
  duplicate registration is rejected.
* All nine interfaces share one graph-scoped resource but have distinct typed
  service descriptors and sink/source lanes.
* Two pure-C++ graph engines run concurrently on different threads with no
  shared mutable connection or callback state.
* Live registration in simulation is rejected and constructs no push source or
  external worker.

Core behavior
~~~~~~~~~~~~~

* Exact and wildcard subscriptions preserve concrete subjects, reply subjects,
  payload bytes, exact-case headers, empty values, and repeated values.
* Equal subscription keys share one subscription; sharing identities create
  independent subscriptions; queue groups deliver each broker message to one
  local/remote group member according to NATS semantics.
* Core publishes distinguish local acceptance, maximum-payload rejection,
  reconnect-buffer rejection, and closed-connection rejection without claiming
  subscriber delivery.
* Unary requests distinguish response, no responders, timeout, replacement,
  client removal, and transport failure.  A late reply cannot revive an old
  generation.
* Received requests can be answered by composing subscription output with the
  ordinary Core publish service and the message's reply subject.
* Slow-consumer drops are typed, counted, and never silent.  The default graph
  failure policy stops after reporting them.

JetStream behavior
~~~~~~~~~~~~~~~~~~

* The extension binds an existing compatible durable pull consumer and creates
  an explicitly requested graph-owned ephemeral pull consumer; it does not
  mutate stream administration state.
* Pull batch count, maximum bytes, expiry, acknowledgement wait, maximum
  deliveries, and ``MaxAckPending`` are honored and observable.
* Message, metadata, and acknowledgement token tick together.  Stream and
  consumer sequences, stored timestamp, delivery count, pending count, domain,
  subject, headers, and payload round-trip.
* No ack occurs before the graph supplies an ack request.  ``Ack``, delayed
  ``Nak``, ``Term``, and ``InProgress`` have their documented final/non-final
  token behavior.
* Unknown, stale, cross-path, and duplicate-final tokens are rejected without
  acknowledging a different delivery.
* JetStream publish reports include stream, sequence, domain, duplicate status,
  expected-sequence failure, timeout, and async transport failure.
* Stable message ids and expected sequence controls round-trip without being
  relabeled as exactly-once processing.
* Unacknowledged work is redelivered by JetStream after failure according to
  consumer policy.

Key/Value and typed TSD cache behavior
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A raw KV watch emits every latest matching entry, exactly one public
  snapshot/live transition even for an empty bucket, and then live puts,
  deletes, purges, and expiry markers in watcher order.
* Exact, wildcard, multi-filter, shared, and independent watch identities are
  covered.  ``UpdatesOnly`` omits the image; ``Resume`` begins at the requested
  revision; invalid option combinations fail before opening a watcher.
* Put, create, CAS update, delete, and purge return attributed results and
  revisions.  Create/update conflict, not found, oversize value, bucket limit,
  timeout, and transport failure are distinguishable.  The service never
  creates or mutates bucket configuration.
* A late ``subscribe_tsd`` against history depth one reconstructs the current
  ``TSD`` from full child images.  It emits no partial value before the watch
  barrier and applies the complete initial image on one graph cycle.
* A publisher restart with a changed key set stages a new generation.  Before
  head commit consumers retain the old complete image; at commit they replace
  it in one cycle, including removal of every ghost key absent from the new
  image.
* Source changes while an image is staging are conflated behind the head
  barrier.  The committed image equals the graph-thread capture, and the queued
  latest per-key states subsequently move consumers to the then-current source
  without losing a removal.
* Live child add/update/remove converges per key.  Repeated pending updates to
  one key conflate to its latest full image, an update followed by removal
  becomes a tombstone, and removal followed by update becomes the new image.
  Nested non-``REF`` child schemas round-trip through their current-value
  schema rather than through scalar-only special cases.
* One source tick which changes two keys is allowed to appear as two remote
  ticks; a test pins this non-atomic contract so a future optimization cannot
  accidentally advertise event replay semantics.
* Two publishers racing to commit from the same head revision produce one
  winner and one typed conflict.  After head replacement, delayed writes from
  the former generation are observed on the raw watch but never alter the
  subscribed TSD.
* Disconnect, watcher error, resume-history loss, malformed envelope, checksum
  failure, digest/key mismatch, and ambiguous mutation outcome enter
  recovering/failed state.  Recovery retains the last coherent value, ignores
  incomplete generations, and replaces only at a complete fresh image.
* A current-generation status heartbeat keeps a complete subscriber ``Live``;
  clean publisher stop or heartbeat timeout makes it ``Stale`` while retaining
  the value.  Old-generation heartbeats and remote wall-clock skew cannot make
  it fresh.  Resumed current-generation status restores ``Live`` without
  re-emitting an unchanged TSD image.
* Key and child descriptor mismatch, unsupported protocol/codec, ``REF``, and
  a value decoded under the wrong schema fail before partial apply.
* Empty TSD, empty/zero-length child values, partially valid nested children,
  deleted keys, and expired keys round-trip distinctly.
* Image entry/byte, in-flight mutation, dirty-key/byte, and obsolete-generation
  cleanup bounds are exercised at, below, and above their limits.  No bound
  breach leaves the cache claiming ``Live``.
* Typed TSD publication rejects bucket/per-key expiry, while the raw KV API
  proves create-only TTL and expiry-marker behavior independently.

Ordering, lifetime, and flow control
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* One bounded burst push source owns all cross-thread ingress storage.  The
  extension has no second inbound queue, wake token, or executor callback.
* Repeated raw values for one service key are emitted FIFO on consecutive
  cycles; independent keys may share a cycle; no raw value is conflated.
* Stop while callbacks, pull owners, or KV watchers are blocked in sender
  admission returns ``false`` and completes without deadlock.
* Repeated start/stop, partial connection failure, reconnect, subscription/KV
  watch removal/re-add, request/KV timeout races, stale ack tokens, publisher
  replacement, and shutdown timeout pass under AddressSanitizer.  Race-sensitive
  cases run under ThreadSanitizer where the dependency permits it.
* Every queue/buffer limit and refusal path is tested at its boundary.  Core
  loss is reported; JetStream pull stops requesting before exceeding the
  declared in-flight bounds.
* Only graph-thread code applies ``StopGraph``.

Python, packaging, and release
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Python exposes the same named schemas and service semantics, with equivalent
  native C++ tests for every transport-visible behavior.
* Native byte transport, callbacks, and TSD encoding/decoding do not acquire
  the GIL.
* Fake-transport tests cover wiring, lifecycle, refusal, generation, and
  projection without a broker.  Broker conformance tests run against supported
  NATS Server versions and cover reconnect, JetStream redelivery, KV
  snapshot/live handoff, and CAS.
* The extension includes five executable Python examples: Core
  publish/subscribe, unary request/reply, JetStream consume/process/ack with
  publish acknowledgement, raw KV watch/mutate, and TSD publish/subscribe with
  publisher replacement.  Example wiring is broker-independent in the normal
  test suite; broker-backed smoke tests prove the real event and cache paths.
* The source distribution contains examples and the installed native package
  fixture.  Wheel audits find no loader dependency outside the supported
  platform policy.
* The extension native suite, Python 3.14 suite, installed-SDK consumer,
  packaging audit, broker integration tests, complete core native suite, and
  complete non-WIP Python compatibility suite pass before the RFC becomes
  Accepted.

Implementation plan
-------------------

1. Create ``extensions/nats`` with public named schemas, immutable builders,
   nine service descriptors, fake transport seam, CMake package, Python
   package, and installed pure-C++ fixture.
2. Implement one fake-backed multi-interface service with one standard bounded
   burst source.  Prove lazy materialization, path isolation, keyed sharing,
   ordered collision unrolling, generation fencing, and teardown before adding
   the NATS C dependency.
3. Add the NATS C RAII layer, graph-scoped connection, callbacks, Core
   subscriptions, publish reports, request inbox multiplexer, typed events, and
   slow-consumer handling.
4. Add JetStream pull consumers, async publish acknowledgements, ack-token
   registry, all four ack dispositions, and confirmed ack where selected by
   review.
5. Add raw KV handles, snapshot/live watchers, attributed asynchronous
   mutations, CAS, markers, watcher cancellation, and fake/broker conformance.
6. After RFC 0017's converter prerequisite lands, add the versioned generation
   protocol, ``publish_tsd``/``subscribe_tsd``, bounded coherent images,
   per-key dirty-state conflation, schema/digest validation, recovery, and
   publisher fencing/status heartbeats.
7. Add the Python bridge, API documentation, all five runnable examples, and
   package/source-distribution audits.
8. Add reconnect/failure injection, memory/performance
   evidence, full macOS gates, Linux sanitizer validation, and opportunistic
   Windows validation.
9. Update this RFC to match implementation experience and mark it Accepted only
   when the implementation and all acceptance evidence merge.

Implementation status
---------------------

No implementation is part of this proposal.  The repository currently
contains the core service, transport-planning, bounded push-source, extension
registration, and packaging facilities required by the design.  The NATS
extension, types, service interfaces, fake transport, NATS C integration, and
tests remain to be implemented.  The typed TSD cache additionally depends on
the still-Proposed RFC 0017 codec and cannot be reported complete before that
prerequisite is implemented and accepted.

References
----------

* :doc:`rfc_0000` — RFC and extension ownership process.
* :doc:`rfc_0003_extension_scalar_registration` — native/Python scalar
  registration for installed extensions.
* :doc:`rfc_0011_source_only_adaptor_collapse` — shared service/adaptor
  boundary substrate.
* :doc:`rfc_0014_request_reply_transport_planning` — direct transport for
  decoupled external sinks and sources.
* :doc:`rfc_0015_kafka_extension_api` — first-party broker extension precedent
  and the intentionally distinct Kafka contract.
* :doc:`rfc_0017_binary_value_codec` — canonical schema descriptors, binary
  value images, and image/delta framing; a typed-cache prerequisite.
* :doc:`rfc_0027_bounded_push_source_queues` — standard cross-thread sender
  admission and shutdown contract.
* :doc:`research_layered_network_services` — separation of event streams and
  cached-state subscriptions.
* :doc:`../developer_guide/real_time_adaptors` — task ownership, sender, ack,
  and simulation rules.
* :doc:`../developer_guide/services` — authoritative service boundary model.
* `NATS Core publish/subscribe
  <https://docs.nats.io/learn/core-nats/publish-subscribe>`_.
* `NATS subjects and wildcards
  <https://docs.nats.io/learn/core-nats/subjects-and-wildcards>`_.
* `NATS request/reply
  <https://docs.nats.io/learn/core-nats/request-reply>`_.
* `NATS queue groups
  <https://docs.nats.io/learn/core-nats/queue-groups>`_.
* `NATS message headers
  <https://docs.nats.io/learn/core-nats/headers>`_.
* `NATS connection lifecycle
  <https://docs.nats.io/learn/core-nats/connection-lifecycle>`_.
* `JetStream delivery and acknowledgement
  <https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment>`_.
* `JetStream acknowledgement responses and redelivery
  <https://docs.nats.io/learn/jetstream/acknowledgment>`_.
* `JetStream pull consumers
  <https://docs.nats.io/learn/jetstream/pull-consumers>`_.
* `JetStream consumer scaling and MaxAckPending
  <https://docs.nats.io/learn/jetstream/worker-pool>`_.
* `JetStream advanced publishing
  <https://docs.nats.io/learn/jetstream/advanced-publishing>`_.
* `NATS Key/Value overview
  <https://docs.nats.io/learn/key-value/>`_.
* `NATS Key/Value snapshot and live watching
  <https://docs.nats.io/learn/key-value/watching>`_.
* `NATS Key/Value history, revisions, and compare-and-swap
  <https://docs.nats.io/learn/key-value/history-and-revisions>`_.
* `NATS Key/Value TTL and limits
  <https://docs.nats.io/learn/key-value/ttl-and-limits>`_.
* `NATS Key/Value stream representation, delete, and purge
  <https://docs.nats.io/learn/key-value/under-the-hood>`_.
* `Official NATS C client <https://github.com/nats-io/nats.c>`_.
* `NATS C subscription pending limits
  <https://nats-io.github.io/nats.c/group__sub_group.html>`_.
* `NATS C JetStream publish API
  <https://nats-io.github.io/nats.c/group__js_pub_group.html>`_.
* `NATS C JetStream pull subscription API
  <https://nats-io.github.io/nats.c/group__js_sub_group.html>`_.
* `NATS C Key/Value API
  <https://nats-io.github.io/nats.c/group__kv_group.html>`_.
* `NATS C Key/Value watch options
  <https://nats-io.github.io/nats.c/structkv_watch_options.html>`_.
* `NATS C Key/Value watcher API
  <https://nats-io.github.io/nats.c/group__kv_watcher.html>`_.
