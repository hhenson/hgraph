RFC 0022: Serializable Graph and Binding Manifest
=================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-16
:Target: Reproducible graph identity, checkpoint compatibility, and deployment bindings

Summary
-------

Define a stable, serializable manifest for a fully wired hgraph program.  The
manifest describes the graph contract, the concrete implementations selected
at wiring time, resolved schemas and scalar arguments, graph edges, nested
graph templates, extension identities, and external source/sink bindings.  It
is canonical, versioned, and comparable across processes and builds.

The manifest does **not** serialize executable code, function pointers,
credentials, or live resources.  An application reconstructs a graph through
its ordinary C++ or Python wiring path and validates the resulting manifest
before attaching a checkpoint or replay log.  A later factory registry may use
the same manifest to locate a named graph factory, but arbitrary C++ captures,
Python closures, and runtime handles can never be reconstructed from bytes
alone.

This is the identity prerequisite for :doc:`rfc_0023_graph_checkpoint_recovery`.
It is also independently useful for audit, deployment substitution, and
explaining why two nominally identical runs selected different behaviour.

Motivation
----------

The runtime already retains most of the information needed to execute a graph:
``GraphTypeMetaData`` has the flattened node and edge shape, node records carry
resolved endpoint schemas and selected operation tables, graph traits describe
component identity, and ``GlobalState`` holds graph-scoped configuration and
external resource handles.  Those objects are deliberately process-local:
schema and operation-table pointers are interned addresses, runtime type ids
may be process-stable tokens, and ``GlobalState`` may contain credentials or
live resources.

That makes them the correct execution representation and the wrong durable
identity.  Today there is no stable artifact that can answer all of:

* Is this rebuilt graph the one a checkpoint was captured from?
* Which overload and representation strategy did wiring select for each node?
* Which extension versions and public ABIs contributed implementations?
* Which external source and sink bindings must a deployment provide?
* Which scalar policies, type realizations, and run inputs affect
  reproducibility?
* Why did two runs with the same top-level graph name behave differently?

Diagnostic graph dumps and inspector snapshots answer related human questions,
but their ids are runtime creation order, their labels are presentation, and
their format is intentionally not a compatibility contract.  A graph manifest
has the opposite properties: stable, canonical, complete for semantic
identity, and deliberately free of live state.

Ownership boundary
------------------

The manifest schema, canonical encoding, capture, validation, and extension
registration contract belong in core.  Independently built extensions must be
able to contribute nodes and resource bindings that a core graph can identify
without core depending on those extensions.

Applications and extensions own:

* the graph factory or ordinary wiring code used to reconstruct a graph;
* resource locators and deployment-specific binding configuration;
* secrets, credentials, and construction of live service/adaptor resources;
* semantic implementation versions for their registered nodes; and
* migration code when they deliberately support restoring an older contract.

Core never serializes a callable or a live resource.  Core also does not infer
implementation identity from a function address, C++ RTTI name, Python object
id, shared-library path, or diagnostic label.

Conceptual model
----------------

There are three related artifacts rather than one overloaded object:

``GraphManifest``
   Immutable description of the fully resolved wired program: graph templates,
   nodes, edges, schemas, scalar arguments, implementation identities, and
   required bindings.  Its canonical descriptor is the compatibility identity.

``BindingManifest``
   Serializable description of an external binding required by the graph.  It
   identifies the binding kind, public contract, and non-secret resource
   locator.  It never contains the live handle or credentials used to construct
   one.

``RunManifest``
   Per-run reproducibility inputs which do not change graph topology: graph
   manifest identity, executor mode, logical start/end bounds, declared random
   seeds, configuration identity, and the concrete binding manifests selected
   for the run.  Checkpoints and input logs identify a run manifest.

Keeping the run separate means a graph wired once can execute over several
time ranges or environments without pretending those runs are the same
artifact.  Keeping bindings separate means a deployment can substitute an
equivalent service endpoint without changing the graph's computational
contract, while the run manifest still records which endpoint was used.

Public C++ contract
-------------------

The exact container spelling may change during implementation, but the public
semantic surface is:

.. code-block:: cpp

   namespace hgraph::manifest
   {
       struct ManifestId
       {
           std::array<std::byte, 32> sha256;
       };

       class GraphManifest final
       {
         public:
           [[nodiscard]] std::uint16_t format_version() const noexcept;
           [[nodiscard]] ManifestId id() const noexcept;
           [[nodiscard]] std::span<const std::byte> canonical_descriptor() const;
       };

       class BindingManifest final;
       class RunManifest final;

       [[nodiscard]] GraphManifest capture(const GraphBuilder &graph);
       [[nodiscard]] ValidationResult validate(const GraphManifest &expected,
                                               const GraphManifest &actual);

       [[nodiscard]] Bytes encode(const GraphManifest &manifest);
       [[nodiscard]] GraphManifest decode_graph(std::span<const std::byte> bytes);
   }

``ManifestId`` is a lookup handle: SHA-256 over the canonical descriptor with
domain separation by manifest format version.  The descriptor itself is the
identity and is compared on attach, following RFC 0017's schema rule.  A hash
collision therefore becomes a failed descriptor comparison rather than silent
compatibility.

``ValidationResult`` owns path-addressed differences suitable for diagnostics.
Validation never reports only "hash mismatch"; it identifies the first and,
within a configured bound, subsequent incompatible graph, node, schema,
implementation, scalar, edge, extension, or binding entry.

The graph builder retains or can produce its manifest after wiring is complete.
Runtime code does not regenerate it by walking live storage.  Dynamic child
instances use the already-manifested child graph template and are represented
in checkpoints, not added to the immutable graph manifest.

Canonical graph contents
------------------------

Graph templates
~~~~~~~~~~~~~~~

Every static and nested graph template records:

* graph name and semantic graph implementation identity;
* ordered node table;
* ordered edge table, including output/error/recordable-state source kind and
  every source and target path component;
* push-source boundary and other wiring-affecting graph flags;
* referenced nested graph templates, de-duplicated by descriptor identity; and
* the closed type-realization information that changes concrete value storage
  or polymorphic dispatch.

Ordering is the canonical wiring/rank order produced by the graph builder, not
pointer order or hash-container iteration order.  A format implementation must
sort every otherwise unordered map by its canonical encoded key.

Nodes
~~~~~

Each node entry records:

* a manifest-local stable node id and graph-template id;
* node semantic name and kind;
* concrete implementation id and implementation version;
* contributing core or extension package and public ABI identity;
* input, ordinary output, error-output, recordable-state, local-state, and
  scalar schemas where present;
* endpoint roles and forwarding/ownership annotations;
* immutable scalar configuration values;
* readiness, active/valid/structural input selectors and scheduler flags;
* checkpoint capability declared by RFC 0023; and
* node-specific child-template or binding descriptors exposed through the
  public manifest operation table.

The node id is stable within one graph descriptor.  Initially it is derived
from the containing graph-template id and canonical node ordinal.  It is not a
promise that inserting an unrelated node preserves checkpoint compatibility:
the initial checkpoint contract requires exact manifest equality.  A future
migration contract may add explicitly authored semantic ids without changing
the exact-match default.

Node labels remain in the manifest for diagnostics but are not sufficient
implementation identity.  Duplicate labels are legal today and labels may be
edited without changing executable behaviour.

Schemas and scalar values
~~~~~~~~~~~~~~~~~~~~~~~~~

Every attached value and time-series schema uses the canonical structural
descriptor defined by :doc:`rfc_0017_binary_value_codec`, never a registry
pointer or diagnostic type name.  Immutable scalar values use the same
schema-directed binary encoding.  A scalar with no registered durable codec
makes the graph non-manifestable for checkpoint purposes and produces a
path-addressed wiring error.

Values which only affect diagnostics may be marked non-semantic and omitted.
That classification is explicit in the node or binding manifest operations;
the manifest builder does not guess from argument names.

Implementation identity and extensions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Every concrete node or graph implementation that may appear in a durable
manifest publishes a stable identifier:

.. code-block:: text

   package / facility / implementation / semantic-version

For example, an operator overload records the operator contract separately
from the concrete overload implementation selected at wiring.  Two overloads
with identical endpoint schemas but different algorithms therefore do not
share a manifest.

Core implementations use core-owned identifiers.  An installed extension
registers its package id, semantic version, public hgraph ABI requirement, and
manifest operation table with conflict detection.  Registration occurs before
wiring and is captured once; evaluation never consults the registry.

A C++ ``runtime_type_id`` token remains valid for process-local interning but is
not serialized.  A Python implementation supplies a stable registered
implementation id plus its package version.  Module/qualname may be recorded
for diagnostics but is not behavioural identity by itself.  Anonymous lambdas
and closures remain usable in ordinary graphs; they are checkpointable only
when the enclosing application supplies a stable registered implementation
identity and reconstructs the same wiring.

Bindings
--------

A graph boundary requiring an external resource contributes a
``BindingManifest`` through a passive, type-erased operation table.  The common
fields are:

* stable binding id and role (source, sink, service, or adaptor);
* public request/value schema and direction;
* provider implementation and semantic version;
* non-secret resource locator and option identity;
* replay/checkpoint capability;
* external cursor or transaction capability required by RFC 0023; and
* names of required credential classes, never credential values.

A Kafka binding may identify brokers through a deployment alias, topic,
subscription policy, and schema while omitting passwords and live consumer
handles.  A database binding may identify a logical data service and query
contract without embedding a bearer token.  The application resolves those
descriptors to live resources when constructing the run.

Binding equality has two levels:

``Contract``
   The replacement implements the same schemas, direction, checkpoint, and
   effect semantics.  This is sufficient to rebuild the graph.

``Reproducible``
   Contract equality plus the same non-secret resource locator and semantic
   configuration.  This is required to claim that two runs used the same
   external environment.

Checkpoint attachment requires graph contract equality and the binding
compatibility declared by that checkpoint.  Audit comparison may additionally
require reproducible equality.

Manifest operation tables
-------------------------

Generic graph code must not downcast map, mesh, service, Python, or extension
nodes to recover their private builder contexts.  Reusable manifest contracts
therefore live in public headers, with concrete strategies under ``impl``
boundaries.  The node-side table conceptually provides:

.. code-block:: cpp

   struct NodeManifestOps
   {
       void (*append_semantic_descriptor)(const NodeBuilder &, DescriptorWriter &);
       void (*append_child_templates)(const NodeBuilder &, GraphTemplateWriter &);
       void (*append_bindings)(const NodeBuilder &, BindingWriter &);
   };

The table is passive and non-owning.  Its pointer is always non-null; ordinary
nodes use a canonical implementation which emits no additional fields.  The
builder/type record owns or references the stable context for the lifetime of
the graph program.  Manifest capture occurs at wiring time and adds no node
storage or evaluation dispatch.

Determinism and reproducibility declaration
-------------------------------------------

A manifest does not prove that arbitrary code is deterministic.  It records
the declarations and inputs needed to enforce the checkpoint eligibility rules
in RFC 0023.  At minimum a checkpointable graph must declare:

* all non-graph inputs as manifested boundary bindings;
* all host-clock, random, locale, calendar, filesystem, network, and process
  environment dependencies, or that none are used;
* every external sink's replay/effect policy;
* implementation and schema versions; and
* whether Python-authored or extension nodes provide a stable checkpoint
  contract.

Undeclared access through arbitrary user code cannot be detected reliably.
Checkpointability is therefore an explicit capability assembled from the
contributing graph, node, and binding declarations, with conservative refusal
when any required declaration is absent.

Python contract
---------------

Python uses the native C++ manifest implementation.  It receives immutable
wrappers and convenience conversion only:

.. code-block:: python

   manifest = hg.graph_manifest(graph)
   payload = manifest.to_bytes()
   manifest_id = manifest.id

   hg.validate_graph_manifest(expected, graph)

Python does not implement canonicalization, schema identity, hashing, or
validation independently.  Python-authored nodes and graphs contribute their
registered implementation identity through the bridge at wiring time.

Serialization format
--------------------

The canonical descriptor is a versioned, length-delimited binary tree.  It
uses fixed field tags, canonical integer encoding, schema descriptors from RFC
0017, and schema-directed scalar payloads.  Unknown required fields cause a
reader to reject the manifest; explicitly optional extension fields may be
retained and ignored according to their declared compatibility class.

JSON is an optional diagnostic rendering of the decoded manifest.  It is not
the canonical identity and round-tripping through JSON is not required to
preserve byte identity.

Compatibility and migration
---------------------------

Version one uses exact descriptor equality for checkpoint attachment.  No
field-name matching, bundle widening, scalar coercion, implementation fallback,
or automatic overload reselection is attempted.  Positional endpoint and slot
state make a plausible partial match more dangerous than an explicit failure.

Format evolution and graph evolution are separate:

* a manifest format reader may decode older format versions into the same
  semantic descriptor; and
* a graph migration explicitly transforms a checkpoint from one graph manifest
  id to another under RFC 0023.

A migration names its source and target manifest descriptors, is separately
versioned, and is never selected merely because two schemas look compatible.

Security and privacy
--------------------

Manifest capture applies an allow-list contract.  Credentials, tokens, private
keys, live handles, memory addresses, environment-variable values, and arbitrary
``GlobalState`` entries are excluded.  Resource bindings emit their own safe
descriptors; generic manifest code never serializes ``GlobalState`` wholesale.

Diagnostic renderers redact values marked sensitive even if an application
mistakenly attempts to include them.  The binary manifest is still not treated
as a secret store.

Runtime and performance
-----------------------

Manifest synthesis occurs after wiring and before execution.  It may allocate,
sort canonical maps, synthesize codecs, and hash descriptors.  The result is
cached on the graph program and shared by graph instances.

There is no evaluation-path lookup, type switch, hash, allocation, or branch.
Per-run work is limited to composing the already-cached graph identity with
run and binding descriptors.

Implementation stages
---------------------

Stage 1: canonical graph and node descriptor
   Define the public owning values, canonical writer/reader, core node identity,
   schema/scalar encoding, graph templates, edges, and exact validation.

Stage 2: nested templates and implementation registration
   Add ``NodeManifestOps``, built-in nested-node descriptors, selected operator
   overload identities, extension package/ABI identity, and installed-SDK
   registration.

Stage 3: binding and run manifests
   Add the safe binding descriptor contract, core service/adaptor descriptors,
   run reproducibility inputs, security tests, and Python wrappers.

Stage 4: checkpoint integration
   Make RFC 0023 checkpoint and input-log attachment require and retain the
   canonical graph and run manifest identities.

Alternatives considered
-----------------------

Serialize ``GraphBuilder`` or graph storage bytes
   Rejected.  Builders and runtime storage contain function/context pointers,
   allocator-owned containers, subscriptions, and live resource handles.  Their
   memory image is neither portable nor a stable ABI.

Serialize executable callables
   Rejected.  Arbitrary C++ captures and Python closures have no general stable
   reconstruction contract.  Rebuild through ordinary wiring and validate the
   result.

Use graph name and package version
   Rejected.  One graph name can select different overloads, scalar policies,
   schemas, and bindings.  Package version alone cannot identify the wired
   program.

Use diagnostic graph snapshots
   Rejected.  Their ids and paths are runtime diagnostics, and they deliberately
   own rendered current values rather than a canonical executable contract.

Use pointer or registry identity
   Rejected.  Interned pointers and process-stable tokens are ideal hot-path
   handles and meaningless across processes.

Unresolved questions
--------------------

* Whether authored stable node ids are required in the first migration-capable
  format or can follow exact-match checkpointing.
* Whether a named graph-factory registry belongs in core or in deployment
  tooling built on the manifest.
* Which non-secret resource locator fields count toward contract equality
  versus reproducible equality for each core adaptor.
* Whether manifest descriptors should share RFC 0017's outer frame header or
  only its schema and scalar codecs.

Acceptance criteria
-------------------

* The same fully resolved graph built in two processes produces byte-identical
  canonical descriptors and the same manifest id.
* A changed overload implementation, scalar policy, edge path, endpoint role,
  schema flag, nested template, extension version, or binding contract produces
  an unequal descriptor and a path-addressed validation error.
* Graphs whose only difference is pointer address, registry insertion order, or
  hash-container iteration order produce identical descriptors.
* Schema identity uses RFC 0017 canonical descriptors, including every
  wire-affecting flag.
* Credentials, live handles, memory addresses, and unrelated ``GlobalState``
  entries never appear in binary or diagnostic renderings.
* Built-in map, mesh, reduce, switch, service, adaptor, and Python-authored
  graph shapes contribute their nested templates and implementation identities
  without generic code downcasting their private storage.
* An extension registers manifest operations and implementation identity from
  an installed SDK, and a separately built graph round-trips its manifest.
* Python and C++ graph authoring produce the same native manifest for an
  equivalent resolved graph.
* Manifest capture and comparison allocate and run only at wiring/attachment
  time; an evaluation benchmark shows no added tick-path work.

Implementation status
---------------------

Stage 1 is implemented (public headers ``hgraph/manifest/canonical.h``,
``hgraph/manifest/schema_descriptor.h``, ``hgraph/manifest/graph_manifest.h``;
tests ``tests/cpp/test_graph_manifest.cpp``).  Decisions recorded during
implementation:

* **Canonical grammar.**  LEB128 varints, zigzag signed varints, IEEE-754
  little-endian fixed64 floats, varint-length-delimited byte strings and
  nested scopes, ascending fixed field tags.  The writer/reader live in
  ``hgraph::manifest`` free of manifest semantics, answering the open
  question about sharing with RFC 0017: this layer IS the shared
  substrate, defined here first because RFC 0017 is unimplemented.
* **Schema descriptors.**  Recursive structural encoding of
  ``ValueTypeMetaData``/``TSValueTypeMetaData`` — kind, the full flags
  word, atomic identity through a stable wire-atomic enumeration (core
  scalars) or the registered name (extension/python scalars, following
  the named-bundle nominal rule), children, fixed sizes, TSW window
  parameters, named-bundle nominal identity (namespace, local name,
  generic arguments, discriminator).  Synthesis is cold-path and
  computed on demand — no cache, no lock, no registry-reset coupling.
  The conformance reference is ``time_series_schema_equivalent``.
* **Identity id.**  ``ManifestId`` is SHA-256 (self-contained
  ``hgraph/util/sha256.h``; the digest is a lookup/integrity device, not
  a security boundary) over the canonical descriptor with the format
  version as domain-separation prefix; the descriptor bytes remain the
  compared identity.
* **Node identity (stage 1).**  Semantic name (``NodeTypeMetaData``
  display name) plus ``TypeRecord::implementation_label``; the full
  ``package/facility/implementation/semver`` identity and overload
  provenance land with stage 2's registration work.
* **Node labels are NOT in the canonical descriptor.**  A label edit
  must not change the manifest id; labels travel in the diagnostic
  rendering, not the identity.
* **Scalar values.**  Canonical schema-directed encoding covers the
  core atomics including the full engine date/time family (temporal
  RFC 0002 types encode by semantic content — ``ZoneId`` by IANA name,
  never its process-local slot), enums, and tuple/bundle/list
  composites, with set and map content emitted in canonical
  encoded-key order.  A ``WiredFn``
  scalar encodes as its REGISTERED identity — the lifted kernel's
  authored name or the operator marker's name — never a function
  address or RTTI name; an anonymous callable, and any value without a
  canonical encoding (``Any``, queues/cyclic buffers, live handles,
  ``PyNodeRef``), makes the graph non-manifestable with a
  path-addressed ``ManifestCaptureError``.  ``NodeManifestOps``
  (stage 2) supersedes the ``WiredFn`` rule with authored descriptors
  and gives nested-graph owners their child-template encoding.
* **Capture point.**  The finished ``GraphBuilder`` (after
  ``Wiring::finish``/``snapshot``, before ``make_executor``); node and
  edge order is the builder's canonical ranked order.  Node identity
  includes the EFFECTIVE output endpoint annotation (the per-instance
  override, falling back to the node type's schema annotation exactly
  as runtime construction does) and the error-capture options
  (traceback depth, value capture) alongside the capture flag.
* **Strict decoding.**  Every scope field appears exactly once; a
  missing or duplicated required field rejects the descriptor — a
  well-formed frame around an empty or torn descriptor never decodes
  into a valid manifest.

Stages 2–4 (nested templates and implementation registration, binding
and run manifests, checkpoint integration) are not started.

References
----------

* :doc:`rfc_0000`
* :doc:`rfc_0003_extension_scalar_registration`
* :doc:`rfc_0015_kafka_extension_api`
* :doc:`rfc_0017_binary_value_codec`
* :doc:`rfc_0021_recording_versions`
* :doc:`rfc_0023_graph_checkpoint_recovery`
* :doc:`../developer_guide/extension_policy`
