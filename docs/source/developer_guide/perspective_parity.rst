Perspective parity disposition
==============================

This is the disposition for issue #216's API audit against released Python
``hgraph``.  The public package surface is the explicit
``hgraph.adaptors.perspective.__all__`` list.  Names imported by upstream
implementation modules (typing helpers, ``Hg*TypeMetaData`` classes,
Perspective dependency classes and decorators) are not package API and are
recorded in ``tools/parity/surface_known.json`` rather than re-exported.

Public workflows
----------------

The following upstream workflows are retained:

* ``publish_table`` publishes scalar, compound-scalar, ``TSB`` and Arrow
  ``Frame`` TSD values, with scalar, fixed-tuple or compound-scalar keys;
* ``publish_table_editable`` creates an editable table and returns the typed
  ``TSB[TableEdits[K, V]]`` feedback stream.  Its subscription is acquired and
  released with graph lifecycle;
* ``empty_row=True`` preserves the upstream synthetic ``_id`` protocol for row
  insertion, deletion and key edits;
* ``publish_multitable`` is a sink-only service adaptor.  It combines clients
  by key, retaining the upstream unique-key and bundle-race modes without an
  unused reply channel;
* ``PerspectiveTablesManager`` retains table/view configuration, edit
  subscriptions, statistics, temporary-table cleanup, current Perspective
  server/client hosting and Tornado handlers; and
* ``perspective_web`` retains the upstream callable signature and resolves its
  manager through the runtime ``GlobalState`` injectable.

The adaptor interfaces and their default implementations are real native
adaptor/service-adaptor registrations.  Scalar options such as
``index_col_name``, ``history``, ``unique`` and ``empty_row`` are immutable
wiring-time configuration shared by clients at one path; disagreeing clients
are rejected during wiring.

Type-system confirmation
------------------------

``TableEdits`` intentionally mixes scalar and time-series type parameters::

   class TableEdits(TimeSeriesSchema, Generic[K, TIME_SERIES_TYPE]):
       edits: TSD[K, TIME_SERIES_TYPE]
       removes: TSS[K]

This is not a Perspective-only accident.  Released hgraph documents mixed
``TimeSeriesSchema`` generics and uses the same pattern in error-handling and
Arrow schemas, with type-resolution tests.  hg_cpp therefore supports this as
a general schema contract.  Scalar and time-series parameters are resolved by
kind and cross-kind specialisations are rejected.

Perspective row planning uses the public ``hgraph.reflection`` API.  Container
key annotations and ``Frame[Row]`` schemas remain recoverable through that API;
the adaptor does not inspect ``_hgraph`` handles or private ``_TsExpr`` layout.

Deliberate differences
----------------------

Only the current Perspective server/client API supported by the
``perspective-python>=3.8`` extra is implemented.  The upstream compatibility
branch for Perspective 2.10 is not restored.  The manager may additionally be
given an injected compatible client through ``client=`` for deterministic
tests; this is an extension carried through ``**kwargs`` and does not change
the upstream constructor signature.

``TableEdits`` is a wiring annotation, not a scalar value class.  Its synthetic
constructor signature and ``to_scalar_schema`` helper are therefore not part
of the curated contract; edits are created by the graph as a typed ``TSB``.

C++-first correspondence: same behavior, different representation
-------------------------------------------------------------------

Released hgraph already exposes ``publish_multitable`` as a sink-only service
adaptor.  hg_cpp preserves that public contract and behavior.  The differences
described here are internal native representation choices, not parity
deviations.

The native service-adaptor contract represents an input-only interface
directly, so it omits unused ``/to_graph`` storage and registration.  This
changes internal storage and topology only; neither supported public surface
has a reply channel for this adaptor.

Released hgraph carries multi-input client fields on independent stateful
request streams, which retain a static field when another field ticks.  hg_cpp
packs the same fields into one typed native request bundle.  Its transport sends
a full current-state snapshot for the first request and applies deltas
thereafter, reproducing the same retained-field semantics.  Equivalent C++
tests cover multiple clients, repeated cycles and static first-request fields;
the Python tests cover the bridge, row shapes, editable teardown and
multi-table publication.  Only the items explicitly listed under
`Deliberate differences`_ are accepted issue #216 deviations.
