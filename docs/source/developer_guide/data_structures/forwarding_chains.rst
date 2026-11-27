Chained Keyed Endpoints and the Ordered Reduce Tail
===================================================

:Status: Proposed. Awaiting the owner's agreement (hgraph#1932).
:Scope: ``TargetLink`` keyed transitions (TSS, TSD) when one link is bound to
        another, and how ``ordered_reduce`` binds its output.
:Rules: TS-2, TS-7, TS-14, TS-15, TS-17, TS-32, REF-DICTIONARY-RETARGET and
        REF-WITHDRAWAL in ``hgraph_spec`` at ``8f682cf4``.

This page states one contract for links bound to links, and one binding
model for the ordered reduce built on it. It replaces the three models tried
in hgraph#1927. Terms follow :doc:`../binding_vocabulary` and
:doc:`linking_strategies`.

Why the earlier models failed
-----------------------------

A keyed link reports its delta in the slot numbers of an output. In the cycle
it moves it holds two outputs: the one it left, where its removals live, and
the one it reached, where its additions live. A second link bound to the
first one reads it through the first link's *current* slots. That loses
everything the first link holds about the output it left.

Each round of hgraph#1927 moved this defect instead of removing it:

=============================  ==================================================
Model                          What it lost
=============================  ==================================================
Chain every tail               Removals when the zero moved in the cycle the tail
(``ad45e688``, ``b11f76e4``)   left it (F1). Child invalidation of a fixed list,
                               which is read one hop deep (F2, H5).
Resolve every tail             Combiner outputs that are forwarding or REF
(``1cc6baa0``)                 endpoints, and their later moves (R4-F1). Moves of
                               a designation that has never published (R4-F2).
Resolve only the zero          Removals when a combiner endpoint moves (R4-F1,
(round 5 candidate)            dictionary ``switch_``): hgraph#1932.
=============================  ==================================================

Terms
-----

*Endpoint*: anything a link can be bound to. It is a *producer* (an owned
output) or another link. *Chain*: a link, the endpoint it is bound to, and so
on down to a producer. *Move*: a rebind, a sampled bind or a running-graph
unbind of one link. *Before the cycle*: the state at the end of the previous
evaluation cycle, which is what every consumer last saw.

Contract for chained keyed endpoints
------------------------------------

**C1. One delta, defined on membership.** For a keyed link in a cycle:

.. math::

   removed = prior \setminus current \qquad added = current \setminus prior

``current`` is the membership of the producer at the end of the link's chain
now. ``prior`` is the membership the link showed before the cycle. A link
that moved, or whose chain moved beneath it, samples the children of
``current`` (TS-17). No link in the chain has moved: the delta is the
producer's own.

**C2. A move records the producer the link was showing.** On its first move
of a cycle a link walks the endpoint it is leaving and records the producer
it reaches:

- at a link that has already moved in this cycle, continue at that link's
  recorded prior producer;
- at any other link, continue at its current target;
- stop at a producer, or at nothing. A link showing nothing has an empty
  prior.

The record names a producer and never a link. It is taken when the link
moves, so a later move or teardown of the endpoint it left cannot change it.

**C3. The record is anchored at the first move of the cycle.** Later moves
of the same link in that cycle change only ``current``. A link bound for the
first time and moved again in the same cycle keeps its empty prior, so it
reports no removals (C5).

**C4. A link that has not moved reports the delta of the link it is bound
to.** This applies to every removal and addition surface: key, value and item
ranges, ``contains``, the key set, and the slot cursors
(``membership_slot_removed``, ``next_membership_removed_slot``). A slot
number is never carried from one output's slot space into another's. Applied
at each hop, this gives every link above a moved link the moved link's delta.

**C5. A link's own move takes precedence.** A link that moved computes C1
from its own record. It does not replay removals of the endpoint it reached:
its consumer never saw those keys.

**C6. Order and lifetime.** The owner of an endpoint moves every downstream
link off it *before* stopping it. Stopping detaches silently and leaves no
record (the spec's physical teardown). The recorded prior producer stays
readable to the end of the cycle (GRF-25); a retired child graph is destroyed
in a later cycle, as ``ordered_reduce`` already does.

**C7. Notification.** A keyed move stamps the link modified and its key set
modified, and notifies its observers, among them every link bound to it. A
running-graph unbind does so only if the link was showing published state
(TS-15, TS-17).

**C8. Cost.** A move walks the chain once: O(chain depth), fixed at wiring
time. The delta is one pass over the prior producer's slots and one over the
current producer's, with hashed membership tests. Nothing walks the chain per
key.

Required cases
--------------

Each is a regression test for sets and for dictionaries. It observes removed
keys, removed values and items, added keys, membership and the public delta.

==============================================  ==============================
Case                                            Expected
==============================================  ==============================
Inner link withdrawn; outer link stays          Outer reports every prior key
                                                removed, reads invalid, delta
                                                readable (TS-15, C4)
Inner link retargets to an overlapping key set  Keys only in the old output
                                                removed, keys only in the new
                                                one added, overlapping keys
                                                sampled, none removed
                                                (TS-17, C1, C4)
Outer link rebinds after the inner link moved   Outer's prior is the inner
(same cycle)                                    link's prior producer (C2)
Outer link rebinds, then the inner link moves   Outer's prior was recorded at
(same cycle)                                    the rebind (C2)
Outer link rebinds off an endpoint retired in   Rebind first, then stop (C6)
that cycle
Link first bound in the cycle to a target that  No removal reported (C5)
removes a key in that cycle
Link moves twice in one cycle                   Diff against the first prior
                                                (C3)
Three links deep, the innermost moves           Same delta at every level (C4)
==============================================  ==============================

How ``ordered_reduce`` binds its tail
-------------------------------------

The output is a forwarding endpoint tree. Its *tail* is the zero when the
collection is empty, and otherwise the last link's combiner output.

**T1. A zero tail is bound to the zero's producer, leaf by leaf.** The zero
reaches the node through aliases: a from-REF alternative, or an outer
forwarding endpoint. They are resolved, and each output leaf is bound to the
producer. A fixed list or bundle reads child invalidation one hop deep, so
binding the leaf to the producer keeps the removed position and the canonical
delta (H5, F2, TS-32). The tail is a zero tail when the collection is empty
or the combiner returns its accumulator parameter.

**T2. A combiner tail is bound to the combiner's own endpoint.** It is not
resolved, whether or not that endpoint is forwarding, a REF, or currently
unbound. The endpoint moves without the reduce evaluating: ``if_then_else``,
``switch_``, a nested reduce, a ``REF`` element. C4 carries those moves to
the output (R4-F1).

**T3. The zero's designation is observed, not only its value.** The node
watches the zero's REF designation and the from-REF leaves it follows. A
designation that moves between outputs that have never published sends no
value notification, and the node must still rebind (R4-F2, TS-17: losing the
binding detaches the old target). When the observer fires, the zero tail is
resolved again; leaves whose producer is unchanged are left alone.

**T4. Withdrawal of a zero tail reads as a direct REF does.** A keyed leaf
unbinds structurally (TS-15). Any other leaf that had a value unbinds and
wakes its observers without being stamped modified: scalar, dynamic list,
window and reference alike (R4-F3). See the ruling below.

**T5. The tail moves before links are retired.** On a shrink the output is
bound to the new tail and then the removed links are stopped (C6).

**T6. Sampling is unchanged.** Links built in a cycle, and links whose input
moved, sample that input (H1). A tail that aliases an outer input is sampled
after a collection move; a combiner output publishes only when the combiner
ticks (H3). Sampling is decided per forwarding leaf (H6).

Probe matrix
------------

The probes are r4's (``r4_probes.cpp``, ``r4_probes_extra.cpp``). The spike
column is the result with this design applied to ``1cc6baa0``.

=========  ==============================================  =========  ======
Case       What it checks                                  Rule       Spike
=========  ==============================================  =========  ======
#1918      Fold of a re-pointed collection                 T6         pass
H1         Late first selection samples members            T6         pass
H2         Structural REF zero withdrawn; unpublished      T1, T3     pass
H3         Quiet combiner output on a collection move      T6         pass
H4         Keyed REF zero: removals on retarget, withdraw  T1, C1     pass
H5         Scalar cleared in a fixed REF zero              T1         pass
H6         Sibling leaves sampled independently            T6         pass
F1         Zero moves in the cycle the tail leaves it      T1, C2     pass
F2         Fixed-list partial clear; invalid final cycle   T1, TS-32  pass
R4-F1      Forwarding and REF combiner outputs             T2, C4     pass
R4-F1      Dictionary ``switch_`` combiner drops key 9     C4         pass
R4-F2      Unpublished designation withdrawn or moved      T3         pass
R4-F3      Active list or window consumer on withdrawal    T4         pass
new        Tail moves in the cycle a combiner endpoint     C2, C3,    pass
           moves (grow, shrink, overlapping keys)          C6, T5
=========  ==============================================  =========  ======

The last row is not in r4's matrix. It fails with the round 5 candidate even
with the removal delegation of ``b11f76e4``: the reduce moves its tail from
one ``switch_`` endpoint to another while the first one flips, and key 9 is
never removed. C2 fixes the grow case. The shrink case also needs C3 and T5.

Ruling: window and list withdrawal
----------------------------------

Question: R4-F3 wants an active consumer of a dynamic list or window zero
woken when the REF is withdrawn, as a direct REF does. An older probe expects
no public delta in that cycle.

What the pinned rules settle:

- The follower is not valid after the unbind (Binding table), so its value is
  nil and, not being modified, its delta is nil (TS-2).
- Only a keyed withdrawal is *invalid and modified* with a readable delta
  (Binding, TS-15). A list that finishes invalid has no public delta
  (TS-32). A window whose producer invalidates it reads invalid and
  unmodified (TS-7).

So the older probe's expectation, no public delta, is what the rules
require. The two expectations do not conflict: a wake without a modification
stamp satisfies both.

What the rules do not settle: whether the consumer wakes. TS-17 wakes a
*scalar* follower that loses a valid value, and the Binding table says other
unbinds are silent. TS-7 wakes an active input when a producer invalidates a
whole window. This is a question for the owner; the suggestion is to wake
every non-keyed follower that loses a valid value.

What the runtime does today, measured on the spike with a direct
``REF<TSW>`` and no reduce: the consumer wakes and reads *invalid and
modified* with no ``delta_value``, and the output recorder publishes an empty
window replacement. That is a defect of the direct REF path, present on
``main``, and not of the ordered reduce. With T4 the reduce reproduces the
direct REF exactly, defect included. hgraph#1927 therefore asserts parity
with a direct REF for these two kinds, and the direct REF defect is filed and
fixed on its own.

Spike
-----

Branch ``spike/q1932-chain-transition`` in the architect's task tree, on
``1cc6baa0`` with the round 5 candidate. It is evidence, not an
implementation.

- ``target_link.cpp``, ``structural_detach_target``: C2 and C3, about fifteen
  lines.
- ``target_link_ops.cpp``: the removal delegation of ``b11f76e4`` for C4, on
  the three range surfaces only.
- ``ordered_reduce_node.cpp``: T5, binding the tail before
  ``truncate_chain``.

Linux, GCC, Release: the native suite passes apart from the cases listed
under *Not covered*. ``[ordered-reduce-scaling]`` stays flat at 544–620 ns
per element from 500 to 4,000 elements.

Not covered by the spike, and owed by the implementation:

- C4 on ``contains`` and the slot cursors. r2's probe 2
  (``REF<TSD<Int, TSL>>``) still fails; it also fails on ``main`` with a
  direct REF.
- Python probes, ASan, macOS and Windows.
- r4's ADV-2a, 2d, 2e, 9a and 9b fail on ``main``, on every head of
  hgraph#1927 and on the spike alike. They are not in the matrix and concern
  ``indexed_child_at`` on a shared ``REF<TSB>``.

Landing order
-------------

Each step lands green on its own.

1. **hgraph#1932, runtime-wide ``TargetLink`` change.** C1–C8 in
   ``target_link.cpp`` and ``target_link_ops.cpp``; nothing in
   ``ordered_reduce_node.cpp``. Tests are the *Required cases*, written
   against ``switch_``, nested ``switch_`` and a direct REF, with no reduce
   in them. Updates :doc:`linking_strategies` and this page's status. The
   specification gains a chained-endpoint trace under TS-15 and TS-17; that
   change is the owner's.
2. **Direct REF withdrawal of a window or dynamic list** reads invalid and
   unmodified and publishes nothing. Its own issue. Whether the consumer
   wakes follows the owner's answer.
3. **hgraph#1927.** Merge ``main`` after step 1; do not rebase. Apply T1–T6:
   the round 5 candidate plus T5. Add r4's matrix and the new row as C++ and
   Python regression tests. Assert parity with a direct REF for window and
   dynamic list withdrawal. Update ``nested_graphs.rst`` to point here.

Step 3 does not depend on step 2.
