RFC 0014: Automatic Request/Reply Transport Planning
====================================================

:Status: Accepted
:Author: Howard Henson
:Created: 2026-08-04
:Target: Request/reply service wiring, scheduling, and Python compatibility

Summary
-------

Reply-full request/reply services no longer unconditionally insert one
next-cycle request hand-off and one response feedback hand-off. After lazy
service materialization, native wiring selects the least costly transport that
preserves the implementation's dependency structure:

.. list-table::
   :header-rows: 1
   :widths: 32 23 23 22

   * - Implementation shape
     - Request
     - Response
     - Engine latency
   * - Decoupled sink/source
     - direct
     - direct
     - none
   * - Output depends on own request
     - deferred
     - direct
     - one cycle
   * - Calls a service or adaptor
     - deferred
     - feedback
     - two cycles

The selection is automatic. Existing C++ and Python declarations, calls,
registration, ``get_service_inputs``/``set_service_output``, and
``from_graph``/``to_graph`` APIs do not gain a policy argument. The plan is
fixed before graph ranking and introduces no per-tick policy branch.

Motivation
----------

The old reply-full transport assumed that the implementation output could feed
back, directly or through another boundary, to its request input. That was the
safest universal choice and permits recursion, but it charged two cycles to
every implementation.

An external request/reply transport has a different shape. A Kafka-backed
implementation, for example, can sink client requests to an outbound channel
and publish independently received correlated responses through a push source.
There is no graph edge from that response source to the request dictionary.
Neither a request delay nor a response feedback edge breaks a real cycle in
this shape; both are artificial latency.

RFC 0011 made services and adaptors share one boundary model, and RFC 0012 made
reply-less request/reply use the direct keyed-sink relay. This RFC completes
the keyed exchange: a normal request/reply service can now provide the direct
external-transport behavior that previously encouraged users to choose an
adaptor solely for scheduling reasons.

User contract
-------------

No existing user-facing signature changes. Python code continues to declare
``@request_reply_service`` and ``@service_impl`` and may expose implementation
boundaries with either:

* a conventional implementation argument and return value;
* ``get_service_inputs`` and ``set_service_output``; or
* ``from_graph`` and ``to_graph``.

The native equivalents remain ``register_request_reply_service``,
``service::impl_input``/``impl_output``, and the service aliases of
``from_graph``/``to_graph``. All routes lower to the same C++ planner.

A decoupled implementation may send the keyed request dictionary to an
external sink and provide a keyed response dictionary from a push source. The
stable client request id remains the correlation key. External concurrency and
wall-clock timing are not made deterministic by this RFC; the engine preserves
the ordering presented by the constructed graph and the external transport.

Planning model
--------------

Planning occurs after ``Wiring::build_services()`` has materialized every
demanded implementation and before service-rank dependencies are applied. A
wiring-lifetime planner records:

* each pending reply-full client capture;
* the implementation-owned request source for each concrete service path;
* the implementation response port and shared response source; and
* whether the active implementation wired any service or adaptor client.

The implementation output is tested for backward causal reachability to its
own request source. Only ordinary data/rank dependencies participate.
Recovery links, source/capture back-links, and other explicitly rank-free edges
do not establish causality. Structural ports and resolved delayed bindings are
followed so bundles and generic wiring do not hide a dependency.

The decision for one concrete service path is:

``FullFeedback``
   The implementation calls any service or adaptor. The request remains
   next-cycle and the response crosses a feedback pair before entering the
   same-cycle shared-output relay. This conservative rule preserves direct and
   indirect recursion without requiring inter-service whole-graph cycle
   speculation.

``RequestDeferred``
   The response causally depends on the implementation's own request source
   and the implementation has no boundary dependency. Deferring the request
   breaks that cycle; the computed response then publishes directly in the
   implementation cycle.

``Direct``
   The response has no causal path from the request source and the
   implementation has no service/adaptor dependency. The client capture ranks
   before the request source, and the response capture ranks before the shared
   response source, so both relays may publish in their owning cycle.

Key lifetime, request batching, cumulative deltas, response correlation, and
source ownership do not change. This is selection among existing boundary
constructions, not a second request/reply runtime.

Nested clients
--------------

A client dynamically started in a keyed child graph cannot schedule an outer
request source in the current cycle because the outer source's rank may already
have passed. It therefore retains the existing next-cycle outer hand-off even
when the owning implementation selected ``Direct``. The child does not create
or own a response feedback pair; the root implementation plan remains the
single owner of response transport.

Consequences
------------

**Decoupled exchanges gain the direct path immediately.** No opt-in is
required. A request captured in the owning graph can reach its sink in that
cycle, and an externally supplied response can publish in its arrival cycle.

**Self-coupled services lose one artificial cycle.** The observable sequence
for a conventional request-driven implementation changes from
``[None, None, response]`` to ``[None, response]``. The request boundary still
breaks the graph cycle.

**Service-dependent and recursive behavior is retained.** Such an
implementation continues to observe the full two-boundary sequence. The rule
is deliberately conservative: a service call that does not happen to feed the
response still selects full feedback because the referenced implementation may
complete a wider cycle.

**Adaptor scope narrows without a disruptive deprecation.** New keyed,
correlated external exchanges should use request/reply services. Plain
adaptors remain supported for existing APIs and unkeyed merged streams. This
RFC does not emit a deprecation warning; removing or formally deprecating an
adaptor decorator requires a separate compatibility decision.

C++ ownership
-------------

The planner, causal analysis, strategy selection, and synthesized boundary
nodes are C++. The Python bridge supplies erased descriptors and ports to that
contract. Python does not classify the implementation or build a parallel
transport, preserving the repository's C++-first ownership rule.

The reusable erased contract lives in
``include/hgraph/types/request_reply_transport.h``. Its concrete planner and
feedback representation stay under ``src/hgraph/types/impl``. Semantic service
owners depend on the contract and do not name the concrete strategy.

Alternatives considered
-----------------------

Keep full feedback for every reply-full service
   Rejected. It preserves recursion but imposes two cycles on decoupled
   external transports and one unnecessary response cycle on self-contained
   services.

Add a decorator or registration flag
   Rejected. The correct choice follows from the materialized native graph.
   Making users predict internal causality duplicates engine knowledge and
   prevents existing code from receiving the improvement immediately.

Treat every implementation without direct request causality as direct
   Rejected. Calls through services or adaptors can complete a wider dependency
   cycle that local reachability does not expose. The active implementation
   scope therefore records boundary use and selects full feedback.

Always use deferred request and direct response
   Rejected. It removes one cycle but still delays a truly decoupled transport,
   and it does not preserve recursive response cycles.

Acceptance criteria
-------------------

* Public typed C++ wiring proves all three transport selections.
* Matching Python tests prove self-coupled and service-dependent timing and a
  real-time Kafka-style sink/push-source exchange through the existing public
  API.
* Recursive, mapped, meshed, switch, cumulative-delta, removal, and teardown
  behavior remains valid.
* Repeated wiring snapshots do not duplicate synthesized nodes or plans.
* The public C++ installed-SDK consumer builds against the new erased contract.
* The complete native and Python 3.14 compatibility suites pass on macOS and
  GCC 14 Linux; lifetime-sensitive Linux validation includes AddressSanitizer.

Implementation status
---------------------

The typed C++ and erased Python paths use one automatic native planner. Native
and Python focused behavior coverage is implemented. The acceptance evidence
for this implementation is:

* macOS native acceptance: 1,444 tests passed;
* macOS Python 3.14 compatibility: 1,893 passed, 11 skipped;
* installed plain-C++ and wheel-extension SDK consumers passed;
* documentation built with warnings treated as errors;
* GCC 14.2 Linux native acceptance: 1,444 tests passed;
* GCC 14.2 Linux stable-ABI wheel on Python 3.14: 1,893 passed, 11
  skipped; and
* GCC 14.2 Linux AddressSanitizer compatibility: 1,893 passed, 11 skipped,
  with no sanitizer findings.

References
----------

* :doc:`rfc_0011_source_only_adaptor_collapse` — shared service/adaptor
  boundary substrate.
* :doc:`rfc_0012_replyless_request_reply_relay` — direct keyed sink behavior.
* :doc:`../developer_guide/services` — authoritative scheduling matrix and
  user-facing service/adaptor guide.
