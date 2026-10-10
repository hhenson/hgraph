RFC 0048: Reusing stopped nested child graphs
=============================================

:Status: Proposed
:Created: 2026-10-10
:Target: ``map_`` / ``mesh`` key lifecycle, ``switch_`` branch lifecycle, subscription services, ``GraphValue`` / node ops

Problem
-------

Where a keyed or branching operator creates and destroys nested graphs at
runtime, the lifecycle dominates the cell. On main (2026-10-10 profiles):

* ``switch_alternating_branch_sizes_std``: evaluating the selected branch is
  9.8% of the run; constructing the branch graph 38.2% (``GraphValue``
  constructor 22.3%, of which ``construct_node_storage`` 15.8% and
  ``bind_edges`` 10.3%), stopping it 18.3% (``unbind_edges`` 6.9%), starting
  it 10.1%, destroying it 9.1% (``TSInput::~TSInput`` 6%).
* ``service_subscription_py``: ``create_entry_at_slot`` 12%, start 6.8%, stop
  4.8%, and the key-set removal destroying the per-key graph
  (``erase_pending`` → ``composite_destroy``) 5.5% — about 30% of the run is
  building and tearing down one nested graph per subscription key; this is
  the cell where csp still leads (1.37x).
* ``map_`` key churn pays the same create/start/stop/destroy per key.

The construction-side pre-resolution (``perf/node-construction-preresolved``)
trims the per-instance resolution but leaves the structural cost: a nested
graph is a fresh allocation whose nodes construct their inputs and outputs
and bind their internal edges every time a key or branch appears.

Observation
-----------

``map_`` already reuses a stopped graph in one window: a key removed and
re-added before the key set's ``on_erase`` callback "resurrects the same
stopped graph and slot" (``nested_graphs.rst``, ``map_`` slot lifecycle). The
machinery to stop, re-bind and re-start a nested graph in place exists; what
does not exist is a guarantee that a re-started graph behaves as a **fresh**
one when it serves a different key or a different selection, and a place to
keep stopped graphs beyond the erase window.

Contract
--------

1. **Reset protocol.** A node op ``reset_for_reuse`` (default implementation
   on the common node type record, overridable by front-ends) returns a
   stopped node to its post-construction state: outputs invalidated (tracking
   to ``MIN_DT``, value storage destroyed and default-constructed where the
   plan is not trivially resettable), state ``Value`` re-defaulted,
   scheduler state cleared, error and recordable-state outputs invalidated,
   runtime caches and prepared routes already cleared by stop. Internal edges
   stay bound (they are structural to the graph); external bindings are
   unbound by the owner as today. ``GraphView::reset_for_reuse`` applies it
   node by node and resets the graph header (schedule table, next scheduled
   time, evaluation time).
2. **Per-builder pool.** A bounded free list of constructed, stopped, reset
   child graphs keyed by (``GraphBuilder`` identity, type realization),
   owned by the keyed/branching node's storage (not global: lifetimes follow
   the owner). ``create_entry_at_slot`` and ``switch_`` branch selection take
   from the pool before constructing; removal and branch teardown return to
   the pool instead of destroying, up to the bound (default a small constant
   such as 4; ``switch_`` keeps at most one per case). Destruction happens on
   owner teardown and when the bound is exceeded.
   **Retirement precedes reset.** A graph enters the pool only when its
   published endpoints are no longer exposed: ``switch_`` under ``RefCopy``
   keeps the previous branch alive after stop until the new branch supplies
   a valid token, and the map/mesh removal keeps a reference-bearing
   forwarding tree until the key's removal delta has been consumed. Pooling
   reuses that existing retirement step (the retired generation is reset and
   pooled at the point where it is destroyed today), so a transition whose
   replacement terminal is initially invalid still observes the old
   reference until the new one is valid.
3. **Semantics unchanged.** A reused graph is indistinguishable from a
   constructed one: same initial validity, same first-cycle behaviour, same
   checkpoint image (a pooled graph is not part of the image).

Design notes
------------

* Graph storage must be **detached from key-slot lifetime**. Today
  ``InPlaceGraphSlotStore`` keeps a child's graph memory inline in the key
  slot; a pending-erase slot still holds and indexes its old key and is not
  returned to the free list until ``erase_pending`` runs ``on_erase``, so a
  graph parked there can only serve the same key again (the current
  resurrect window) and keeping it parked would block the slot for new
  keys. The pool therefore needs its own graph store: a slot store of graph
  memory indexed from the key entry (one index per entry), with its own
  free list. Key removal follows the documented protocol unchanged (stop,
  unsubscribe, ``on_erase`` destroys the key entry), while the entry's graph
  index is handed to the pool instead of the graph being destroyed; a new
  key takes a pooled graph index or constructs into a fresh graph slot. The
  reuse path is then bind-external-edges plus ``reset_for_reuse`` plus
  start. ``switch_`` already owns its branch memory per case and needs no
  indirection.
* For ``switch_`` the branch graph memory is owned per case; pooling keeps
  the last graph of each case stopped and reset instead of destroying it.
* The reset op is the risk: every node front-end (static, lifted, Python
  compute/generator/sink, service and adaptor nodes, nested operators) must
  either accept the default (re-default planned components) or override.
  Python nodes must drop their handles (generator iterators, callables'
  per-node state) so a reused node re-runs its start hook as a fresh one.
* Memory: a pool holds at most ``bound`` stopped graphs per builder; for a
  churning map with a stable live population this is a constant overhead,
  and for the subscription service it is the recently unsubscribed keys.

Expected effect: ``switch_`` cells where branch construction dominates
should lose most of the 75% lifecycle share; subscription services the
constructed-per-key part of their 30%; key churn the construct/destroy pair.

Acceptance
----------

The nested-graph suites (map, mesh, switch, services) unchanged; new tests
that a reused graph starts fresh (state, validity, scheduler, error output)
for a different key and for a re-selected branch; checkpoint equality across
pool reuse; bake-off A/B on ``switch_alternating_branch_sizes_std``,
``tsd_churn_std``, ``tsd_churn_py``, ``service_subscription_py`` on both
hosts.
