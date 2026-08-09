RFC 0012: A Reply-Less Request/Reply Service Is a Keyed Sink
============================================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-08-02
:Target: Request/reply service transport and scheduling

Summary
-------

A request/reply service declaring no response (an erased
``response_schema == nullptr`` or a typed C++ descriptor with no
``response_schema`` alias) is a **sink**: clients send, the implementation
consumes, nothing comes back. It is nevertheless wired on the request/reply
*transport*, whose request half is
deliberately rank-free and forwards on the **next** cycle. That rank-freedom
exists to permit request/reply **cycles**, which are resolved by the response
feedback edge — and a reply-less service has no response, so it has no cycle to
permit and pays the cost for nothing.

The same shape expressed as a sink-only adaptor uses the rank-correct
same-cycle relay and delivers immediately. This RFC proposes wiring the
reply-less case on that relay, making the two constructs agree.

Measured before this RFC:

.. list-table::
   :header-rows: 1
   :widths: 45 25 30

   * - Shape
     - Client emits at
     - Implementation sees at
   * - reply-less request/reply service, root client
     - cycles 0, 2
     - cycles **1, 3** (+1 cycle)
   * - sink-only adaptor
     - cycles 0, 2
     - cycles **0, 2** (same cycle)

Released hgraph additionally establishes the nested lifecycle boundary: a
reply-less client dynamically started inside ``map_`` at cycles 0 and 2 hands
its requests to the outer service source at cycles **1 and 3**. The proposal
changes the root case only; it preserves that nested deferral.

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

The cost is not merely cosmetic. The runtime records the reason the reply-full
request path is rank-free:

.. code-block:: text

   // Request/reply transport is ordered by its response feedback edge, not
   // by indirect service dependencies. This permits request/reply cycles.

The request capture takes its source with ``rank_dependency = false``, so
nothing orders the implementation after its
clients; correctness comes instead from the request stub forwarding on the next
cycle. For a service with a response that is a sound trade — it is what makes a
service able to call itself. For a reply-less service there is no response
feedback edge at all: implementation registration returns before building one.
The rank-freedom therefore buys nothing for a root client and the extra cycle
is pure latency there.

Proposal
--------

When the response is omitted (``response_schema == nullptr`` in the erased
descriptor, or no typed ``response_schema`` alias), wire a root request
transport the way the adaptor input relay is wired:

* the capture is registered as a sending service client rank, so ``finish``
  orders the **source** after the capture; and
* the capture node is built with ``same_cycle``, so it schedules the paired
  source for the **current** evaluation time.

The capture's own input on the source stays ``rank_dependency = false``. That is
not an oversight: a capture must not wait on the source it feeds, and making it
a ranked dependency produces a wiring cycle. Ordering flows the other way,
through the existing service-rank contract.

A dynamically-started nested capture still schedules the outer source for
``evaluation_time + MIN_TD``. Its owner can begin after that outer pull source's
rank has already passed, so current-cycle publication would be unsafe without a
different nested-boundary construction. This is also released hgraph behavior
and is part of the compatibility contract, not an implementation gap hidden by
the root optimization.

Nothing else changes: the transport is still keyed by client id, still a
``TSD<Int, request_schema>``, still built by ``keyed_request_input_source_node``.
This is a scheduling and rank-ordering change to one flavour of one boundary,
not a new mechanism — the relay it moves onto is the one RFC 0011 made shared.

Consequences
------------

**Timing (intended).** A reply-less request/reply implementation observes a
root client's request in the cycle the client sent it, instead of the next one.
A dynamically-started nested client retains the one-cycle outer hand-off. Both
cases are directly observable and pinned against released hgraph.

**Rank ordering (intended).** The implementation is ordered after every client
that sends to it in the owning graph, in the same way a sink-only adaptor's
implementation is. A nested hand-off continues to rely on its explicit cycle
boundary.

**Root cycles through a reply-less service become a wiring error.** Today a
graph in which a reply-less service's implementation ultimately publishes back
to that same root service is tolerated, because the next-cycle boundary breaks
the loop silently. Ranked ordering reports it as a cycle at wiring time.

This is the one genuinely behaviour-narrowing consequence and the main thing to
review. The position taken here is that it is a **correction**: such a
configuration is a real dependency cycle, and reporting it beats silently
inserting a cycle of latency. Nothing in this tree relies on it. If a concrete
use emerges, the answer is an explicit opt-in rather than restoring the implicit
next-cycle behaviour for everyone.

**Reply-full request/reply was outside this RFC.** RFC 0014 subsequently
replaced its unconditional full-feedback transport with automatic planning,
while retaining full feedback for service-dependent and recursive
implementations.

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

* A reply-less request/reply implementation observes a root client's request
  in the **same** cycle the client sent it. A test asserts the observed cycle
  numbers, not merely the values — the existing coverage
  (``test_replyless_request_reply_service_captures_requests``) asserts values
  only and passes either way.
* A dynamically-started ``map_`` client hands its request to the outer source
  on the **next** cycle, matching released hgraph; both Python and native C++
  regressions assert the cycle numbers.
* The same root graph expressed as a sink-only adaptor and as a reply-less
  request/reply service delivers on the **same** cycle, pinned by one test that
  evaluates both.
* Multiple clients at one path are still keyed by client id, and their requests
  still combine into one cumulative dictionary delta.
* A reply-*full* request/reply service is unchanged: same timing, and direct and
  mapped recursion still wire.
* A root dependency cycle through a reply-less service is reported at wiring
  time with the ordinary cycle diagnostic.
* The typed C++ descriptor may omit ``response_schema``. Its implementation
  consumes ``TSD<Int, request_schema>`` as a sink, ``wire<Service>`` returns
  ``void``, and no reply endpoint is required or built.
* ``services.rst`` scheduling matrix records the reply-less case on the
  same-cycle relay rather than under request stubs.
* The full native suite and the non-WIP Python suites pass, including
  ``python/tests/ported``.

Implementation status
---------------------

**Implemented.** The erased and typed C++ surfaces share the existing request
transport. A reply-less descriptor selects same-cycle capture plus a sending
service-rank dependency, omits the reply source/feedback path, and returns no
client port. Root and nested timing are covered in Python and native C++.

Two findings from implementing it:

* **The rank direction is the opposite of the obvious one.** Making the
  capture's source input a rank dependency produces a wiring cycle: the capture
  must NOT wait on the source it feeds. The relay pattern keeps that input
  rank-free and lets the sending service-rank relation order the *source* after
  the capture. The cycle detector caught this immediately.
* **The runtime already supported it.** ``make_request_input_capture_node``
  takes a ``same_cycle`` flag, added for rank-correct service-adaptor captures;
  the reply-less case just had to pass it. Wiring-level ordering alone changed
  nothing, because the next-cycle behaviour lives in the capture node's
  schedule-time computation. That computation deliberately retains the nested
  ``+MIN_TD`` hand-off, matching released hgraph.

No existing test needed changing, including
``test_replyless_request_reply_service_captures_requests``, which asserts values
rather than cycles — which is precisely why the latency went unnoticed, and why
the new tests assert observed cycle numbers.

Final validation counts are recorded on the pull request after the acceptance
gates complete.

References
----------

* :doc:`rfc_0000` — RFC process.
* :doc:`rfc_0011_source_only_adaptor_collapse` — the unified boundary model this
  extends, and whose "explicitly out of scope" note this supersedes for the
  reply-less case.
* :doc:`rfc_0014_request_reply_transport_planning` — the later automatic
  transport selection for reply-full services.
* ``docs/source/developer_guide/services.rst`` — the scheduling matrix
  distinguishing same-cycle relays from next-cycle request stubs.
