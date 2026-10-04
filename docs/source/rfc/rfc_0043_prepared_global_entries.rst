RFC 0043: Prepared global entries
=================================

:Status: Proposed, implementation under review
:Created: 2026-10-03
:Target: Native node preparation and GlobalState

HGL's accepted keyed-state profile resolves keys and exact types before any
node starts. Generated hooks must not repeat string lookup or type dispatch.
This extends the existing GlobalState store; it does not add another store
or change the ordinary keyed Python interface.

Contract
--------

``GlobalStateView::prepare(key, binding)`` binds one existing store entry to
an exact ordinary schema and returns a borrowed ``PreparedGlobalEntry``.
Preparation validates seeds and requires one exact storage binding per key.
Repeated preparation with a different physical representation is rejected,
even when its ordinary schema is identical; cached access plans therefore
remain valid for every handle. Existing seeds of that schema are retained into
the first binding, and ordinary keyed writes continue to convert into it.
Preparation leaves an
unseeded entry absent. Equal keys share the same stable value cell. Binding
metadata is separate from value presence and does not change the value schema.

The prepared handle reads the cell or replaces it with an independently
retained value. Callers supply values of the already checked schema. Access
does not look up keys or dispatch on a value's type. Reads of absent cells
throw; failed retention leaves the old cell unchanged. Writing an absent
(typed-null) view is rejected rather than retained as a default-constructed
value, so the store's size and membership stay exact. Handles cannot outlive
their store. Moving an owning ``GlobalState``, replacing it by copy/move assignment,
or destroying it invalidates all handles borrowed from that owner, as with
other value views. Such replacement is permitted only outside an active run;
re-prepare fresh handles against the replacement before use. Copy construction
creates an independent store and never retargets an existing handle.
Ordinary keyed writes preserve a prepared entry's type; erasing
or wholesale replacing prepared storage is rejected.

An optional native ``prepare(const NodeView &)`` callback runs after graph
attachment and before start. It may inspect immutable scalar configuration,
prepare value plans, and bind global entries into node-local cache. It must
not publish, schedule, or read temporal inputs. Failure aborts construction
before start. Generated start hooks preserve these prepared cache fields.

Validation
----------

Tests cover shared keys, incompatible seeds, absent versus empty values,
independent runs, retained copies, stable handles across store growth,
failure before start, and registry-free repeated access. Installed-SDK
compilation covers the public C++ API. Existing Python keyed access and
copy-in/copy-out behavior remain covered by the compatibility suite.

A GCC 15.2 x86-64 release run (nine samples of 100,000 operations) measured
prepared scalar set/get at 12.508, 12.509, 12.529 and 12.497 ns per operation
with 1,024, 2,048, 4,096 and 8,192 unrelated keys respectively. Every case
allocated zero bytes. Repeat using ``hgraph_type_erasure_perf`` with
``HGRAPH_TYPE_ERASURE_PERF_FILTER=prepared_global_entry``. This checks access
cost as the store grows; it does not claim aggregate copies have constant cost.

Wiring declarations and seed ownership
--------------------------------------

``Wiring::prepare_global_entry(key, binding)`` declares the exact type for
one run without changing the live owner seed. Repeated declarations require
the identical storage binding, including its representation. ``has_global_entry`` exposes
these declarations to cold recorder-key selection. Root and statically known
child wirings share only this declaration plan; independent roots have fresh
plans. A child cannot introduce a new declaration after the consuming root
finish. Repeated snapshots realize separate copies and leave the wiring open.

Declaration validates any currently visible seed. Root finish validates again
against the final copied seed and prepares stable entries in the builder-owned
store. Nodes acquire their prepared handles from that runtime-owned copy.
The owner's store remains unprepared, so established lower copy-out succeeds
and a later run can bind the same key to a different type after replacing its
seed. Declaring an absent entry neither inserts a value in the owner nor
publishes one in the graph. Prepared runtime handles and their lifetimes do
not extend into the wiring declaration plan.
