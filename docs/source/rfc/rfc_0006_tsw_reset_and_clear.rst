RFC 0006: TSW Reset and Clear
=============================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-07-25
:Target: TSW mutation and ``to_window``
:Related: RFC 0007

Summary
-------

Add an explicit clear operation to a time-series window and an optional reset
signal to ``to_window``.  A reset clears the retained observations before any
source tick delivered in the same evaluation cycle.  The operation is visible
as a TSW modification even when the window was already empty.

This RFC deliberately does not redesign the TSW delta or record/replay format.
Until that work is completed, a clear-only TSW tick cannot be represented by
the existing scalar-addition delta.  RFC 0007 specifies the follow-on design
work for duration eviction, multi-value removal deltas, and recovery.

Motivation
----------

``TSW`` is the graph-level owner of observations retained by a window.  Window
consumers should not reproduce that history in private node state merely to
support a logical reset.  Rolling calculations instead compose:

.. code-block:: text

   source -> admission/lag -> to_window -> private calculation node

The private node may retain derived computational state, but the TSW remains
the authoritative tick history.  Reset and invalidation policies therefore
need a standard way to clear the TSW rather than replacing it with a
node-specific queue.

Ownership boundary
------------------

The mutation primitive and ``to_window`` overload are domain-independent core
facilities.  Policies deciding *when* to reset, whether another output is
invalidated, and how a statistic reacts remain graph or downstream concerns.

C++ contract
------------

``TSWDataMutationView`` gains:

.. code-block:: cpp

   void clear();

``clear``:

* destroys all retained value and timestamp elements;
* retains the configured period and allocated capacity;
* marks the TSW modified at the mutation scope's evaluation time, including
  when it was already empty;
* produces an empty current window value;
* does not report the cleared values through the legacy singular
  ``removed_value`` surface; and
* may be followed by exactly one ``push`` through the same mutation scope.

A push followed by clear, two pushes, two clears, or a clear followed by a push
through a different mutation scope at the same evaluation time is rejected.
The permitted clear-then-push sequence provides the atomic ordering required by
``to_window`` when reset and source tick together.

The typed static-node output convenience ``Out<TSW<...>>::clear()`` delegates to
the same mutation operation.

Operator contract
-----------------

``to_window`` retains its existing forms and adds an optional signal:

.. code-block:: text

   to_window(ts, period, min_window_period=period)
   to_window(ts, period, min_window_period=period, reset=signal)

Both tick-count and duration-valued periods support the reset form.  ``reset``
is a ``SIGNAL``: any reset tick clears the window; a boolean value carried by a
``TS[bool]`` bound as a signal is not interpreted.

Evaluation ordering is:

1. if reset ticked, clear the TSW;
2. if ``ts`` supplied a valid tick, append that tick; and
3. publish the resulting TSW modification.

Consequently, simultaneous reset and source ticks produce a one-element window
containing the current source value.  A reset-only cycle produces an observable
empty-window tick.  Window readiness is recalculated from the cleared contents;
the next source tick starts warm-up again.

The reset input is an optional overload input rather than a per-tick scalar
policy.  Existing calls and their resolved output schemas remain unchanged.

Python contract
---------------

Python wiring exposes the same named input:

.. code-block:: python

   values = to_window(ts, 20, min_window_period=5, reset=reset)

Python user nodes observe the same empty value and modification semantics as
C++ nodes.  There is no independent Python reset implementation.

Representation and performance
------------------------------

Clear destroys the live cyclic-buffer or duration-queue elements in place and
retains allocation.  Its cost is ``O(size)`` because element destructors must
run; it performs no additional allocation.  The ordinary push path remains
unchanged.  A boolean carried by the short-lived mutation view records whether
that scope performed the permitted clear-before-push sequence.  The storage
reuses the existing last-eviction timestamp with an absent evicted value to
mark the most recent clear.  This lets legacy delta capture reject
clear-plus-push as well as clear-only ticks without allocation or changing the
TSW physical layout.

Compatibility and persistence restriction
-----------------------------------------

Existing source-only ``to_window`` calls are source- and behaviour-compatible.
The TSW current value can already represent an empty list, but the canonical
TSW delta is currently only the newly appended scalar.  It cannot distinguish:

* no TSW tick;
* a clear-only TSW tick; and
* a scheduler-driven removal-only TSW tick.

Therefore phase-one resettable TSWs are not delta-record/recover safe.  A
recorder asked to capture any TSW tick containing a clear, whether clear-only
or clear followed by push, must fail explicitly rather than silently omit the
clear.  Full-current-value inspection remains valid.  RFC 0007 will define a
versioned structured delta and the associated migration.

This restriction is intentional: clear is needed to establish correct graph
and node ownership now, while inventing a partial persistence encoding would
make the later duration-eviction migration harder.

Alternatives considered
-----------------------

Private queues in every rolling node
   Rejected.  They duplicate TSW semantics, prevent standard composition, and
   make recovery dependent on each consumer.

Rebuild a switched nested graph for each reset
   Rejected as the primitive contract.  Branch construction samples held
   inputs and introduces lifecycle and allocation semantics unrelated to
   clearing a window.

Replace the TSW from an erased empty list
   Possible internally, but unsuitable as the public authoring API and unable
   to express the permitted atomic clear-then-push sequence clearly.

Acceptance criteria
-------------------

* Fixed and duration TSW mutation tests cover non-empty clear, empty clear,
  retained capacity, rejection of invalid same-cycle operations, and an atomic
  clear followed by push.
* Native C++ wiring tests cover reset-only, source-only, and simultaneous
  reset/source cycles for tick and duration periods.
* Python wiring tests prove the same behaviours through the native operator.
* Existing ``to_window`` and TSW tests remain unchanged and pass.
* Unsupported clear-only and clear-plus-push delta capture are tested as
  explicit errors.
* The complete native and non-WIP Python compatibility suites pass.

Implementation status
---------------------

Implemented on the proposal branch, pending review and acceptance:

* fixed- and duration-window storage implement observable, capacity-retaining
  clear;
* typed and erased mutation APIs expose clear;
* ``to_window`` has reset-aware tick-count and duration overloads in C++ and
  Python wiring; and
* native and Python tests cover reset ordering and the explicit legacy-delta
  limitation.

References
----------

* :doc:`rfc_0000`
* :doc:`rfc_0007_scheduled_duration_tsw_eviction`
