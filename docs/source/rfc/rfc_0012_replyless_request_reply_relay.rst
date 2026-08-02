RFC 0012: A Reply-Less Request/Reply Service Is a Keyed Sink
============================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-02
:Target: Request/reply service transport and scheduling

Summary
-------

A request/reply service declaring no response (``response_schema == nullptr``)
is a **sink**: clients send, the implementation consumes, nothing comes back. It
is nevertheless wired on the request/reply *transport*, whose request half is
deliberately rank-free and forwards on the **next** cycle. That rank-freedom
exists to permit request/reply **cycles**, which are resolved by the response
feedback edge — and a reply-less service has no response, so it has no cycle to
permit and pays the cost for nothing.

The same shape expressed as a sink-only adaptor uses the rank-correct
same-cycle relay and delivers immediately. This RFC proposes wiring the
reply-less case on that relay, making the two constructs agree.

Measured, on the current tree:

.. list-table::
   :header-rows: 1
   :widths: 45 25 30

   * - Shape
     - Client emits at
     - Implementation sees at
   * - reply-less request/reply service
     - cycles 0, 2
     - cycles **1, 3** (+1 cycle)
   * - sink-only adaptor
     - cycles 0, 2
     - cycles **0, 2** (same cycle)

Motivation
----------

RFC 0011 established that the service and adaptor surfaces are one boundary
model and unified the relay both are built from. It scoped the other flavours
out on the grounds that *"the difference is the explicit stub protocol, not the
relay"*. Steps 3 and 4 of that RFC removed the stub-protocol difference —
services gained ``from_graph``/``to_graph`` and a by-stub registration — so the
stated reason no longer holds and the remaining flavours are worth revisiting.

Taking the flavours as a grid of **direction × keying** makes the gap obvious:

.. list-table::
   :header-rows: 1
   :widths: 22 26 26 26

   * - Keying
     - Output only
     - Input and output
     - Input only
   * - none / merged
     - reference service **=** source-only adaptor *(unified, RFC 0011)*
     - duplex adaptor
     - sink-only adaptor
   * - by client id
     - —
     - request/reply service **=** service adaptor
     - **reply-less request/reply**
   * - by user key
     - —
     - subscription service
     - —

The bottom-right and top-right cells are the same construct at different
keyings: many clients into one implementation, nothing returned. They should
differ in *how they key*, not in *when the implementation sees the data*.

The cost is not merely cosmetic. ``service_runtime.cpp:566`` records the reason
the request path is rank-free:

.. code-block:: text

   // Request/reply transport is ordered by its response feedback edge, not
   // by indirect service dependencies. This permits request/reply cycles.

The request capture takes its source with ``rank_dependency = false``
(``service_runtime.cpp:551``), so nothing orders the implementation after its
clients; correctness comes instead from the request stub forwarding on the next
cycle. For a service with a response that is a sound trade — it is what makes a
service able to call itself. For a reply-less service there is no response
feedback edge at all (``register_request_reply_service_impl`` returns before
building one, ``service_runtime.cpp:611-619``), so the rank-freedom buys
nothing and the extra cycle is pure latency.

Proposal
--------

When ``response_schema == nullptr``, wire the request transport the way the
adaptor input relay is wired:

* the request capture takes the request source as a **ranked** dependency
  rather than ``rank_dependency = false``; and
* the pair is declared with ``Wiring::add_same_cycle_pair``, so ``finish``
  validates the order and the capture schedules the source for the **current**
  evaluation time.

Nothing else changes: the transport is still keyed by client id, still a
``TSD<Int, request_schema>``, still built by ``keyed_request_input_source_node``.
This is a scheduling and rank-ordering change to one flavour of one boundary,
not a new mechanism — the relay it moves onto is the one RFC 0011 made shared.

Consequences
------------

**Timing (intended).** A reply-less request/reply implementation observes a
client's request in the cycle the client sent it, instead of the next one. This
is the point of the change, and it is directly observable by any graph that
measures delivery latency.

**Rank ordering (intended).** The implementation is ordered after every client
that sends to it, in the same way a sink-only adaptor's implementation is. That
is a stronger guarantee than today's, where ordering is unconstrained and
correctness rests on the cycle boundary.

**Cycles through a reply-less service become a wiring error.** Today a graph in
which a reply-less service's implementation ultimately publishes back to that
same service is tolerated, because the next-cycle boundary breaks the loop
silently. Ranked ordering will report it as a cycle at wiring time.

This is the one genuinely behaviour-narrowing consequence and the main thing to
review. The position taken here is that it is a **correction**: such a
configuration is a real dependency cycle, and reporting it beats silently
inserting a cycle of latency. Nothing in this tree relies on it. If a concrete
use emerges, the answer is an explicit opt-in rather than restoring the implicit
next-cycle behaviour for everyone.

**Reply-full request/reply is untouched.** Its rank-freedom and response
feedback are exactly what make recursive request/reply legal, and
``test_service_wiring.cpp`` pins that behaviour.

Alternatives considered
-----------------------

Leave it; document the latency
   Rejected. Two spellings of "many clients into one implementation, nothing
   returned" differing by a cycle is precisely the kind of accidental
   divergence RFC 0011 exists to remove, and the slower one is the one a user
   reaches for when they want a *service*.

Make the reply-less case a sink-only adaptor internally
   Rejected. A sink-only adaptor *merges* clients into one stream, while the
   request/reply transport keys them by client id. The keying is the useful
   part and must survive; only the scheduling should change.

Give the whole request/reply flavour the same-cycle relay
   Rejected. The rank-free request path is load-bearing when a response exists:
   it is what permits request/reply cycles, which
   ``test_service_wiring.cpp`` covers for both direct and mapped recursion.

Opt in through a flag on the interface
   Deferred. It adds surface for a choice that follows from the descriptor —
   a service either declares a response or it does not. A flag becomes
   worthwhile only if the cycle-detection consequence above turns out to block
   a real graph.

Acceptance criteria and test plan
---------------------------------

* A reply-less request/reply implementation observes a client's request in the
  **same** cycle the client sent it. A test asserts the observed cycle numbers,
  not merely the values — the existing coverage
  (``test_replyless_request_reply_service_captures_requests``) asserts values
  only and passes either way.
* The same graph expressed as a sink-only adaptor and as a reply-less
  request/reply service delivers on the **same** cycle, pinned by one test that
  evaluates both.
* Multiple clients at one path are still keyed by client id, and their requests
  still combine into one cumulative dictionary delta.
* A reply-*full* request/reply service is unchanged: same timing, and direct and
  mapped recursion still wire.
* A dependency cycle through a reply-less service is reported at wiring time
  with the ordinary cycle diagnostic.
* ``services.rst`` scheduling matrix records the reply-less case on the
  same-cycle relay rather than under request stubs.
* The full native suite and the non-WIP Python suites pass, including
  ``python/tests/ported``.

Implementation status
---------------------

Not started. This RFC is the first commit on its branch, per
:doc:`rfc_0000` workflow step 1.

The branch is stacked on the RFC 0011 implementation
(``agent/rfc-0011-unify-service-adaptor-surface``, PR #263), which touches the
same ``register_request_reply_service_impl`` body; the dependency is one of
merge order, not of design.

References
----------

* :doc:`rfc_0000` — RFC process.
* :doc:`rfc_0011_source_only_adaptor_collapse` — the unified boundary model this
  extends, and whose "explicitly out of scope" note this supersedes for the
  reply-less case.
* ``docs/source/developer_guide/services.rst`` — the scheduling matrix
  distinguishing same-cycle relays from next-cycle request stubs.
