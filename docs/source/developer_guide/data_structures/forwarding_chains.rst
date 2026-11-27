Links Bound to Links, and the Ordered Reduce Tail
=================================================

:Status: Proposed, revision 2. Awaiting the owner's agreement (hgraph#1932).
:Scope: A ``TargetLink`` whose target is another ``TargetLink``, and how
        ``ordered_reduce`` binds its output.
:Rules: TS-2, TS-7, TS-14 to TS-17, TS-32, TS-35, GRF-25, GRF-27,
        REF-DICTIONARY-RETARGET and REF-WITHDRAWAL in ``hgraph_spec`` at
        ``8f682cf4``.

Revision 1 kept reads flowing through chains of links and tried to make the
chain report the right delta. Review (r3, on hgraph#1933 at ``95504cfb``)
showed that this cannot be made cheap or correct. This revision changes the
model: **a link follows an endpoint and holds a producer. Nothing reads
through another link.**

Terms follow :doc:`../binding_vocabulary` and :doc:`linking_strategies`.
*Endpoint*: anything a link can be bound to, a producer (an owned output) or
another link. *Move*: a rebind, a sampled bind or a running-graph unbind.
*Before the cycle*: the state at the end of the previous evaluation cycle.

What is wrong today
-------------------

A link bound to a link reads it hop by hop. Three defects follow. The code
paths are on ``main``; the measurements below are r3's, on the revision 1
spike.

- **Cost.** Every read pays every hop, and a keyed child lookup dispatches
  twice per hop: :math:`\Theta(K \cdot 2^d)` for K keys at depth d. A reduce
  whose combiner passes its accumulator on has depth proportional to its
  size. A chain of 18 links took 1.2 s; 100 links did not finish in ten
  minutes.
- **Slot spaces.** A keyed link reports its delta in the slot numbers of an
  output. In the cycle it moves it holds two outputs. A link above it reads
  one set of slots and loses the other, or pairs a slot from one output with
  the key table of the other.
- **Stopped endpoints.** A link above a stopped link reads nothing, although
  the consumer still holds what the stopped link showed (GRF-25).

The model
---------

**P1. Two facts per link.** A link records the *endpoint it follows* and the
*producer it shows*. The endpoint is what the owner bound it to and what
``bound_output`` and ``forwarding_target`` report. The producer is the owned
output at the end of the chain, or nothing. For a link bound straight to a
producer the two are the same and nothing changes.

**P2. Everything is read from the producer.** Value, delta, membership, child
lookup, the key set, slot observers and tick subscription all use the
producer. No operation of a link calls the same operation on another link.
A read costs the same at any depth. ``resolve_forwarding_source`` returns the
recorded producer in one step.

**P3. A move re-points the followers.** A link keeps the links that follow
it. When its producer changes in a running graph it tells each follower,
which takes the new producer as a move of its own and tells its own
followers. A move between two outputs that have never published re-points
followers too; it sends no value notification. A plain bind while wiring or
starting is adopted quietly.

**P4. One record per link per cycle, anchored at the first move.** On its
first move of a cycle a link records the producer it was showing and its own
modification stamp. Later moves in that cycle change only the current
producer. The record names a producer, never a link, and needs no walk: the
link already holds it.

**P5. The delta is defined on membership.**

.. math::

   removed = prior \setminus current \qquad added = current \setminus prior

``prior`` is the recorded producer's membership before the cycle and
``current`` the membership of the producer now. A link that moved samples the
children of ``current`` (TS-17). Every test for "was this link showing
published state" is made on the recorded prior producer, not on whatever the
link holds after the move.

**P6. Ending the cycle where it began is not a move.** If the current
producer equals the recorded prior producer, the record is dropped and the
link's stamp restored; a tick of that producer in the cycle is kept (TS-16).
The exception is an owner sampling the endpoint the link already follows:
that is a request and it samples.

**P7. A stop is silent and is not passed on.** Stopping a link detaches it
and tells its followers only that it is gone. They keep the producer they
show, and their subscription to it, until their owner moves them or the
producer is destroyed (GRF-25). Destruction unbinds them silently through the
existing observer lifetime protocol. No owner has to move downstream links
before stopping an endpoint, which it cannot do across a ``REF`` boundary or
for a consumer of higher rank (GRF-27).

**P8. Removals are reported by key.** A removal surface is a range of keys,
values or items that carries its own slot space. Links do not offer
``membership_slot_removed`` or ``next_membership_removed_slot``: a slot
number from the prior producer must never index the current one.
``capture_delta`` and the Python delta export read the ranges.

**P9. Notification.** A keyed move stamps the link and its key set, notifies
the link's observers and notifies the observers of single keys (TS-35),
withdrawal included.

**P10. Cost.** A read is O(1) in depth. A move costs one step per link that
shows the moved producer, which is the number of endpoints whose content
changed. A tick notifies each link that shows the producer once. Nothing is
proportional to depth times keys. The implementation proves this with a
doubling benchmark (100 to 1,600 links) that prints the cost per link for
growth, for a tick and for a move; each must stay flat.

Alternatives
------------

=============================  ==============================================
Model                          Trade-off
=============================  ==============================================
Read through the chain, with   Keeps today's structure. Needs a second
a cache per cycle              delta surface for links in transition, and a
                               cache whose validity depends on every link
                               below. Does not fix stopped endpoints.
                               Revision 1; rejected.
Resolve at bind and do not     One hop, no bookkeeping. Loses every later
follow (``1cc6baa0``)          move of the endpoint (R4-F1, R4-F2). Rejected.
Follow the endpoint, hold the  One hop for reads. Costs a follower list per
producer (this page)           link and one step per follower on a move.
                               A moved endpoint with many followers pays for
                               all of them at once, where a chain paid on
                               each read.
=============================  ==============================================

The follower list is the existing idea of the structural slot-observer list
(:doc:`linking_strategies`): empty and single forms in one tagged pointer.

Required cases
--------------

For sets and dictionaries, observing removed keys, values and items, added
keys, membership, single-key observers and the public delta. None uses a
reduce.

==============================================  ==============================
Case                                            Expected
==============================================  ==============================
Inner link withdrawn; outer link stays          Every prior key removed; outer
                                                invalid, delta readable
Inner link retargets to an overlapping key set  Old-only keys removed, new-only
                                                added, overlap sampled
Outer rebinds after inner moved (same cycle)    Diff against what outer showed
                                                before the cycle
Outer rebinds, then inner moves (same cycle)    The same
Inner withdraws, then outer unbinds or moves    Prior keys still removed (P5)
to an unpublished target (same cycle)
One link moves twice, through unpublished       Prior keys still removed (P4)
targets
Inner moves A to B, outer rebinds to A          No delta (P6)
Endpoint stopped before a ``REF`` republishes   Removals from the stopped
(``switch_`` reference output)                  endpoint's producer (P7)
``map_`` child stopped at a lower rank than     The same (P7)
its consumer
``TSD<K, TSL>`` retarget with a vacated key     Exact removed keys (P8)
Single-key observer on a withdrawn dictionary   Woken, reads absent (P9)
Link first bound to a target that removes a     No removal reported
key in that cycle
Three links deep, the innermost moves           Same delta at every level
==============================================  ==============================

How ``ordered_reduce`` binds its tail
-------------------------------------

The *tail* is the zero when the collection is empty or the combiner returns
its accumulator parameter, and otherwise the last link's combiner output.

**T1. A zero tail shows the zero's producer, leaf by leaf.** Aliases (a
from-REF alternative, an outer forwarding endpoint) are resolved and each
output leaf is bound to the producer. Fixed lists and bundles need this
today because non-keyed links still read one hop (H5, F2, TS-32).

**T2. A combiner tail follows the combiner's own endpoint.** It is never
resolved by the reduce, whether the endpoint is forwarding, a ``REF`` or
unbound. P3 carries the endpoint's later moves to the output (R4-F1).

**T3. The zero's designation is observed at every hop.** The node watches
each ``REF`` designation and each forwarding leaf on the zero's alias chain,
not only the first. The chain's depth is fixed at wiring time. A designation
that moves between unpublished outputs must still rebind the tail (R4-F2).

**T4. Withdrawal of a zero tail reads as a direct REF does**, for every
leaf kind (R4-F3). See the ruling below.

**T5. The tail is bound once per cycle, after the chain is reconciled.**
Revision 1 bound it before retiring links. P7 makes that unnecessary (the
spike passes the shrink case without it), and it caused a second move in the
cycle.

**T6. Sampling is unchanged** (H1, H3, H6).

T1 and T3 are interim. When non-keyed links follow the same model (step 4
below) the tail simply follows the zero endpoint and both rules go.

Ruling: window and list withdrawal
----------------------------------

R4-F3 wants an active consumer of a dynamic list or window zero woken when
the ``REF`` is withdrawn. An older probe expects no public delta then.

- Settled by the pinned rules: no public delta. The follower is not valid
  after the unbind, so its value and delta are nil (Binding table, TS-2).
  Only a keyed withdrawal is invalid and modified (TS-15). TS-32 and TS-7 say
  the same for a list that finishes invalid and a window its producer
  invalidates.
- Not settled: the wake. TS-17 names a scalar follower; the Binding table
  says other unbinds are silent; TS-7 wakes an active input when a producer
  invalidates a window. The question is with the owner.
- The two expectations do not conflict. A wake without a modification stamp
  satisfies both.

Today a direct ``REF<TSW>`` or dynamic ``REF<TSL>``, with no reduce, reads
invalid and *modified*, and the window publishes an empty replacement
(hgraph#1934). T4 would make the reduce copy that defect, so hgraph#1934
lands before hgraph#1927.

Evidence
--------

Spike ``4dbfcd7d0`` on branch ``spike/q1932-chain-transition`` in the
architect's task tree, on ``1cc6baa0`` with the round 5 candidate. It
applies P1 to P5 and P7 to keyed links only, P9 for withdrawal, and attempts
P6 and P8. Linux, GCC, Release. It is evidence, not an implementation.

What it shows:

- r4's matrix passes: #1918, H1 to H6, F1, F2, R4-F1 (dictionary ``switch_``
  included), R4-F2, R4-F3.
- r3's probes pass for same-cycle withdrawal and double moves (R3-1, R3-2,
  diag-2b), stopped endpoints (R3-4a, R3-4b, c6a, c6b, diag-4c) and
  single-key observers (R3-5a, R3-5b). ``switch_node.cpp`` is not changed.
- r2's probe 2 (``REF<TSD<Int, TSL>>`` retarget) passes; it fails on
  ``main``.
- The exponential cost is gone. r3's chain of 18 links: 1,182 ms before,
  0.45 ms after.
- A move at the bottom of a keyed chain is linear in the links re-pointed:
  185, 412, 1,069 and 2,176 µs for 100, 200, 400 and 800 links.

What it does not show:

- **Per-link cost is not yet flat.** Growing the chain one link per cycle
  costs 35, 45, 69, 111 and 191 µs per link at 100 to 1,600 links, and a tick
  of the zero 1.0, 1.3, 1.8, 2.5 and 3.9 µs per link. Three walkers were
  found and removed (key-set and slot subscription through the chain,
  ``resolve_forwarding_source``, ``structural_sample_time``). What remains is
  not identified; no profiler was available. P10 is therefore a requirement
  on the implementation, not a measured result.
- **P6 and P8 are specified, not proven.** R3-3 (return to the same
  producer) and R3-6 (``TSD<K, TSL>`` with a vacated key) still fail.
- **R3-7b fails**: the spike keeps the candidate's one-hop zero observer.
  T3 states the fix.
- **Three existing tests regress** on the spike: a set recovery scenario
  (``test_component_recovery_scenarios.cpp:268``), checkpoint restore of a
  reduce with a ``REF`` zero (r2's probe 6) and "structural REF unbind
  publishes only previously visible removals"
  (``test_std_operators.cpp:5348``). Each concerns what a link showed before
  the cycle after a restore or an unbind. They are part of the acceptance
  floor.
- In the spike a follower of a stopped link holds its producer without its
  own lifetime subscription. P7 requires that subscription.
- Not run: Python, ASan, macOS, Windows. Non-keyed links are unchanged.
- r4's ADV-2a, 2d, 2e, 9a and 9b fail on ``main`` and here alike
  (hgraph#1935).

Landing order
-------------

Each step lands green on its own.

1. **hgraph#1932: keyed links follow P1 to P10.** Files, as far as the
   required cases need them: ``target_link.{h,cpp}``,
   ``target_link_ops.cpp``, ``ts_output/forwarding.h``, the input set and
   dictionary views and ``ts_input/base_view.cpp``, ``ts_delta.cpp`` (P8),
   and the Python set and key-set delta export in
   ``ts_input_conversions.cpp`` and ``conversion.cpp`` (P8). No change to
   ``ordered_reduce_node.cpp`` or ``switch_node.cpp``. Tests: the required
   cases in C++ and Python, r3's probes, the doubling benchmark of P10, and
   the existing suite. Updates :doc:`linking_strategies`. A chained-endpoint
   trace under TS-15 and TS-17 goes to the specification in a cross-linked
   pull request; that change is the owner's.
2. **hgraph#1934: direct REF withdrawal of a window or dynamic list** reads
   invalid and unmodified and publishes nothing. Waits for the owner's answer
   on the wake.
3. **hgraph#1927.** Merge ``main`` after steps 1 and 2; do not rebase. Apply
   T1 to T6: the round 5 candidate, T3 at every hop, and no early tail bind.
   Add r4's matrix, the tail-moves-while-an-endpoint-moves case and R3-7 as
   C++ and Python regression tests.
4. **Non-keyed links follow the same model** (scalar, window, reference,
   signal, dynamic list). A new issue. It removes the remaining per-hop reads
   and the scalar forwarding special cases, and lets the reduce drop T1 and
   T3. Not needed by step 3: those chains exist on ``main`` today and
   hgraph#1927 does not lengthen them.
