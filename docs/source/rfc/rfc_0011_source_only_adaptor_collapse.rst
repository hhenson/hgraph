RFC 0011: Source-Only Adaptors Are Reference Services
=====================================================

:Status: Proposed
:Author: Howard Henson
:Created: 2026-08-02
:Target: Public C++ and Python service/adaptor wiring surface

Summary
-------

A **source-only adaptor** — an ``adaptor::interface`` (Python ``@adaptor``) that
declares an output and no input — is the same construct as a **reference
service**. Both are "one implementation produces a value, many clients consume
it, published through a shared-output relay".

They are not merely similar in spirit. The relay construction is *identical*:
the same two node builders, the same wiring schema, the same
``rank_dependency = false`` on the capture's second input, the same
``add_same_cycle_pair`` contract, the same path-as-intern-key model — and on the
erased runtime side, literally the same two C++ functions serve both families
already (``src/hgraph/types/service_runtime.cpp:136-178``). What differs is a
path string, a marker ``typeid``, and an error message.

Around that identical core, the two surfaces have drifted in four ways
(publishing contract, publish enforcement, output-schema checking, and generic
client schema). The reference-service behaviour is the stricter one for concrete
schemas, so the collapse tightens checking rather than merely renaming — but the
**generic** template path is currently as unchecked as the adaptor path, so
closing that hole is part of the work rather than a property inherited for free.
All four are enumerated and resolved below.

This RFC proposes collapsing them, in two stages and in this order:

1. **Lift, additively.** Bring the adaptor's usage model onto the service
   surface — client scalar options, time-series registration configuration, and
   the ``from_graph`` / ``to_graph`` publish model. Nothing breaks; services
   gain capability. The aim is the **union** of the two usage models, not the
   intersection.
2. **Collapse, subtractively.** With nothing left that only an adaptor could do,
   deprecate and then remove the source-only adaptor spelling. The reference
   service is the primitive.

The adaptor surface remains for the cases that genuinely differ from any service
flavour — duplex (input and output) and sink-only (input, no output).

Ordering matters: doing stage 1 first means the migration in stage 2 never asks
anyone to give something up, and every step is independently reviewable.

Motivation
----------

**The duplication is exact.** ``service::detail::reference_shared_output_source``
(``include/hgraph/types/service_wiring.h:678-701``) and
``adaptor::detail::output_source`` (``include/hgraph/types/adaptor_wiring.h:344-366``)
have the same body:

.. code-block:: cpp

   // service_wiring.h — reference service
   std::string full_path  = reference_output_path<Service>(user_path);
   const auto *target_meta = resolved_schema_meta<output_schema>(
       user_path.resolution, "reference service output");
   const auto *ref_meta   = TypeRegistry::instance().ref(target_meta);
   WiringNodeSchema schema;
   schema.output = ref_meta;
   schema.state  = ref_meta->value_schema;
   WiringPortRef port = w.add_node(
       std::type_index(typeid(reference_output_source_marker)), schema,
       std::span<const WiringPortRef>{}, path_key_value(full_path),
       [path = std::move(full_path), target_meta]() {
           return make_shared_output_source_node(path, *target_meta);
       });

   // adaptor_wiring.h — source-only adaptor: the same, with
   //   adaptor_to_graph_path<Interface>  instead of reference_output_path<Service>
   //   output_source_marker              instead of reference_output_source_marker
   //   "adaptor output"                  instead of "reference service output"

``capture_reference_service_output`` (``service_wiring.h:876-910``) and
``adaptor::detail::capture_output`` (``adaptor_wiring.h:368-398``) differ in the
same three ways and nothing else: both build the two-source input (the second
with ``rank_dependency = false``), both call ``make_shared_output_capture_node``,
both call ``add_same_cycle_pair(capture, shared_output)``.

The client sides match too. ``adaptor::detail::client_to_graph``
(``adaptor_wiring.h:427-440``) and ``service::reference_service``
(``service_wiring.h:1010-1024``) each register a client path, wire the shared
output source, register a receiving client rank, and return the port ``as`` the
declared output schema. The runtime node builders are the same
(``make_shared_output_source_node`` / ``make_shared_output_capture_node``), the
scheduling contract is the same (rank-correct, same-cycle relay), and the
descriptor is already one struct — ``RuntimeServiceDescriptor``
(``include/hgraph/types/service_runtime.h:43-61``) carries an optional
``input_schema`` whose own comment reads *"adaptor output reuses output_schema;
either may be null for sink-/source-only adaptors"*.

**This violates a standing project guardrail.** ``CLAUDE.md`` §3(iii) —
"Parallel abstractions. v2 principle: one runtime model, no generic fallback …
Two ways to do one thing is the smell to kill, not add to."

**It has already produced a defect.** Because a source-only adaptor satisfies
both ``adaptor::detail::adaptor_interface`` and
``service::detail::reference_service_interface``, ``wire<Interface>`` is an
ambiguous partial specialization in any translation unit that includes both
headers:

.. code-block:: text

   error: ambiguous partial specializations of 'wire_customization<TickAdaptor, void>'
     note: adaptor_wiring.h:696 matches [with Interface = TickAdaptor, ...]
     note: service_wiring.h:2073 matches [with Service  = TickAdaptor, ...]

``reference_service_interface`` excludes a *duplex* adaptor through
``!has_adaptor_input_schema``, but nothing excludes a source-only one.
``tests/cpp/test_adaptor_wiring.cpp`` never trips this only because it does not
include ``service_wiring.h``. This is a symptom to remove, not to patch: with
one construct there is one specialization and no ambiguity.

Scope
-----

In scope:

* **Stage 1 — lifting** the adaptor usage model onto the service surface: client
  scalar options, time-series registration configuration, and the
  ``from_graph`` / ``to_graph`` publish model. This applies to services
  generally, not only to the case being collapsed.
* **Stage 2 — collapsing** the **source-only** case: an interface declaring an
  output and no input.

Explicitly out of scope, because the transports genuinely differ:

* **Duplex and sink-only adaptors.** No service flavour models "graph sends a
  value out and nothing comes back", so ``sink_adaptor`` and the duplex
  ``from_graph``/``to_graph`` pair keep the adaptor surface.
* **Service adaptors versus request/reply services.** These overlap but are not
  identical. ``service_adaptor_from_graph``
  (``src/hgraph/types/service_runtime.cpp:1093-1109``) wires a
  ``keyed_request_input_source_node`` behind an explicit implementation stub,
  while ``register_request_reply_service_impl`` (``:534-583``) auto-wires
  ``request_input_source_node`` plus a response feedback pair. The difference is
  the explicit stub protocol, not the relay. Recorded here as an adjacent
  observation for a possible later RFC; this RFC does not touch it.

Ownership boundary
------------------

The construct is owned by the C++ wiring core. Python's ``@adaptor`` /
``@reference_service`` decorators are frontends over the same erased
``RuntimeServiceDescriptor`` and the same registration entry points, so the
Python change follows the C++ one rather than defining a second contract.

No new runtime node type, ops-table slot, or storage shape is introduced by
either stage. Stage 1 adds **wiring-time API only** — it reuses the relay nodes,
scope machinery and client-config store that already exist, and changes nothing
on the per-tick path. Stage 2 removes code paths.

Proposed C++ contract
---------------------

**Removed.** ``adaptor::interface`` specialisations declaring ``output_schema``
and no ``input_schema`` stop being valid adaptor interfaces:
``adaptor::detail::adaptor_interface`` is narrowed to require
``has_input_schema``. Consequently these overloads lose their source-only forms:

* ``adaptor::adaptor<Interface>(Wiring &)`` and its ``AdaptorPath`` overload
  (``adaptor_wiring.h:642-658``);
* the ``!has_input_schema && has_output_schema`` branch of
  ``register_automatic_adaptor`` (``adaptor_wiring.h:522-527``);
* ``adaptor::detail::output_source`` / ``capture_output`` /
  ``client_to_graph`` in their standalone-source role — they remain only as the
  output half of a duplex adaptor.

**Retained, unchanged.** Duplex and sink-only adaptors; ``from_graph`` /
``to_graph``; ``register_adaptor`` / ``register_adaptors`` /
``register_automatic_adaptor`` for those shapes; every service flavour.

**The replacement** is the existing reference-service surface, with no new API:

.. code-block:: cpp

   // before — source-only adaptor
   struct TickFeed : adaptor::interface
   {
       static constexpr std::string_view name{"tick_feed"};
       using output_schema = TS<Int>;
   };
   adaptor::register_adaptor<TickFeed, TickFeedImpl>(w);
   auto ticks = wire<TickFeed>(w);

   // after — reference service
   struct TickFeed
   {
       static constexpr std::string_view name{"tick_feed"};
       using output_schema = TS<Int>;
   };
   service::register_reference_service<TickFeed, TickFeedImpl>(w);
   auto ticks = wire<TickFeed>(w);

The client call site ``wire<TickFeed>(w)`` is unchanged. The interface loses its
``adaptor::interface`` base and the registration verb changes. The
implementation keeps its freedom to own a push source, since both flavours are
inlined into the top-level graph (``docs/source/developer_guide/services.rst``,
"Implementations are inlined, not nested").

The implementation *body* changes only for manually published adaptors — see
"Publishing contract" below.

Because the source-only adaptor was the only reason an ``adaptor::interface``
could lack an input, narrowing the concept also makes
``reference_service_interface`` and ``adaptor_interface`` provably disjoint, and
the ambiguous ``wire_customization`` disappears without a tie-break rule.

Four real differences to resolve
--------------------------------

Beyond naming, the two constructs differ in four ways. Each needs a decision,
and three of them make the collapse an improvement rather than a pure rename.

**1. Publishing contract: by-return versus by-stub.**
``register_reference_service`` (``service_wiring.h:1042-1055``) wires the
implementation and captures its **returned** ``Port``.
``adaptor::register_adaptor`` (``adaptor_wiring.h:466-485``) instead requires the
implementation to call ``adaptor::to_graph`` itself;
``register_automatic_adaptor`` (``:493-535``) is the closer analogue and calls
``to_graph`` on the implementation's behalf.

*Proposed:* **support both styles on the service surface** rather than forcing
by-return (see "D. The ``from_graph`` / ``to_graph`` usage model for services").
An automatic source-only adaptor migrates with no body change; a manual one
changes ``adaptor::to_graph<Iface>`` to ``service::to_graph<Service>``. Neither
requires an implementation rewrite.

**2. "Did the implementation publish?" is enforced differently.** The adaptor
path opens a ``service_implementation_scope`` with required endpoints
(``adaptor_wiring.h:451``, ``:507-508``), so ``end_service_implementation``
(``graph_wiring.cpp:2213-2243``) fails an implementation that never called
``register_service_implementation_stub`` for each endpoint.
``register_reference_service`` opens no scope.

*Proposed:* **not** to reuse the stub scope. The existing helper cannot express
this contract: ``wire_service_graph_with_scope`` (``service_wiring.h:996-1008``)
returns ``void`` and delegates to ``wire_service_graph``, which discards the
implementation's port (``:983-994``, ``static_cast<void>(wire<Impl>(...))``),
and the scope checks only stub registration — which a by-return implementation
never performs. A non-empty required-endpoint set would therefore reject every
valid by-return implementation, and an empty set would check nothing while
discarding the port needed for capture.

For a by-return implementation the equivalent of "did you publish?" is simply
"did you return a usable port", which ``register_reference_service`` already
answers through ``Port::as<OutputSchema>()`` and, on the erased path,
``describe_service_output``. So this is a **smaller gap than first drafted**:
the enforcement exists in a different form rather than being absent.

Once services support by-stub publishing (D), the two enforcement modes pair
naturally with the two publish styles — the required-endpoint scope for by-stub
implementations, returned-port validation for by-return ones — and no
return-preserving scope mechanism is needed. What remains genuinely missing is
the generic case, which is difference 3.

**3. Output-schema conformance is checked on only one side — but only for
concrete schemas.** The erased reference path always checks, via
``describe_service_output`` (``service_runtime.cpp:189-211``). On the C++
template path the check is conditional:

.. code-block:: cpp

   // service_wiring.h:403-429 — wire_service_impl
   if constexpr (schema_descriptor<OutputSchema>::is_concrete())
   {
       return output.template as<OutputSchema>();      // validates
   }
   else
   {
       return Port<OutputSchema>{w, output.erased()};  // NO validation
   }

For a **generic** C++ reference interface the non-concrete branch wraps
``output.erased()`` without calling ``Port::as`` or comparing against
``resolved_schema_meta``, and ``capture_reference_service_output`` then builds
the capture from the implementation's schema (``:888-893``). That is exactly the
unchecked mismatch this RFC attributes to adaptors
(``service_runtime.cpp:897-900``, ``:995-999``).

*Proposed:* the collapse adopts the checked behaviour **and closes the generic
hole** — the non-concrete branch must compare the implementation's resolved meta
against ``resolved_schema_meta<OutputSchema>`` before returning. Without that,
migrating a generic source-only adaptor would preserve the defect rather than
fix it, and a concrete-only mismatch test would pass while the generic path
stayed unchecked. The test plan therefore requires a **generic** mismatch case,
not just a concrete one.

**4. Generic interfaces return a different schema.** For a non-concrete output
schema, the reference client patches the port schema down to the resolved
value meta (``service_wiring.h:1023-1025``); the adaptor client leaves the
``REF`` meta (``adaptor_wiring.h:440``). Both erased runtime forms patch
(``service_runtime.cpp:268``, ``:948``), so this asymmetry exists only on the
template path.

*Proposed:* adopt the patching behaviour (the erased runtime already agrees with
it). A generic source-only adaptor's client port therefore changes observable
schema — this needs an explicit migration test, not just a compile check.

Capabilities to lift onto services
----------------------------------

A full audit of the adaptor surface found three capabilities with no
reference-service equivalent. **The ruling is to lift the first two onto the
service surface rather than restrict either construct** — the goal is the union
of both usage models, not the intersection. The collapse then removes a
duplicate spelling without removing a capability.

This makes the RFC additive first and subtractive second, which is also a better
migration story: by the time source-only adaptors are removed, everything they
could do a reference service can do.

**A. Client scalar options.** Adaptor clients may pass wiring-time scalar
options that reach the registered implementation, with cross-client agreement
enforced (``python/hgraph/_wiring/_services.py:52-88``,
``_record_adaptor_client_config``, reached only from
``_AdaptorClientStub._prepare_client_request`` at ``:649``):

.. code-block:: python

   raise WiringError(
       f"{stub.flavour.replace('_', ' ')} '{stub.__name__}' clients at "
       f"path {path!r} disagree on wiring-time option(s) {differences!r}")

``_ServiceStub.__call__`` (``:493-551``) never records config. For a
**source-only** interface this matters most: the client's entire call is a path
plus scalars, so this is its *only* parameterisation channel.

*Lift.* The mechanism is **already flavour-generic, has no C++ component, and
is half-done**. The config key is
``(identity, stub.flavour, stub.__name__, stub._specialization, path)``
(``:78-81``); the consumer ``_bind_registered_impl`` (``:1380-1401``) reads it
through ``_adaptor_client_config(matched_stub, path)`` with no adaptor-specific
logic; and the store is a plain Python dict — ``_ADAPTOR_CLIENT_CONFIGS`` has no
occurrence anywhere in ``src/``, ``include/`` or ``python/*.cpp``. The
implementation-consumption half already works for services:
``register_service(path, impl, **kwargs)`` (``:1809``) forwards scalar config
today.

What is adaptor-specific is only the client *call site* — the recorder is
reached solely from ``_AdaptorClientStub._prepare_client_request`` (``:649``).
``_ServiceStub._bind_call`` (``:477-491``) already builds the ``bound``
signature object and discards all but path and requests, so recording from
``_ServiceStub.__call__`` is a small change. The "clients disagree on
wiring-time option(s)" diagnostic already interpolates ``stub.flavour`` and
reads correctly for services unchanged.

**B. Time-series registration configuration.** ``manual_adaptor``
(``_services.py:1177``) lets ``register_adaptor(...)`` take TS-valued kwargs and
wire them as implementation inputs (``_adaptor_registration_inputs``
``:1075-1090``, forwarded as ``inputs=`` at ``:1071``). The bridge's
``register_service_impl`` (``python/py_state_services.cpp:349-371``) has **no**
``inputs`` parameter at all — only the adaptor family and
``register_multi_service_impl`` do. In-tree users:
``python/tests/test_hgraph_api.py:1269-1280`` and ``:1288-1297``.

*Lift.* Three edits, each mirroring an existing adaptor equivalent:
``register_service_impl`` gains an ``inputs`` parameter matching
``register_adaptor_impl`` (``py_state_services.cpp:464-476``);
``_register_resolved_service`` (``_services.py:1785-1806``) forwards it as
``_register_resolved_adaptor`` does (``:1068-1072``); and
``_adaptor_registration_inputs`` (``:1075-1090``) is generalised so a service
implementation may declare time-series parameters supplied at registration. The
helper is already generic apart from its ``implementation.manual_adaptor`` gate
— for services the gate becomes "the implementation declares time-series
parameters that the interface does not supply". On the C++ template surface,
``register_reference_service`` already threads ``Args...`` through to the
implementation; today those are scalars only, and the same pack carries ports.

**C. Catch-all implementations.** ``register_unbound_adaptor_impl``
(``src/hgraph/types/service_runtime.cpp:1032-1090``) is the sole caller of
``Wiring::register_catch_all_service_implementation_candidate``
(``graph_wiring.cpp:1892-1910``) and has no flavour at all — it sweeps
``service_client_records()`` and claims every unclaimed endpoint. Python reaches
it through ``@adaptor_impl(interfaces=())``; ``@service_impl(interfaces=())`` is
not supported. **Two production users**:
``python/hgraph/adaptors/tornado/http_server_adaptor.py:453-454`` and
``websocket_server_adaptor.py:437-438``.

Catch-all is not removed by this RFC — it belongs to the adaptor family, which
survives. But it *discovers* clients by splitting the
``adaptor://…/from_graph|/to_graph`` grammar
(``http_server_adaptor.py:470``, ``websocket_server_adaptor.py:453``,
``_perspective_adaptor.py:182``), so moving any client to ``ref_svc://`` changes
what it can see. The migration must confirm no catch-all is expected to serve a
migrated source-only client.

D. The ``from_graph`` / ``to_graph`` usage model for services
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The adaptor's explicit publish/consume model is the other half of the ruling:
services should offer it too. This turns out to be **almost entirely a naming
and registration problem, not a mechanism problem**, because the service
equivalent already exists.

``service::impl_output<Service>`` (``service_wiring.h:1108-1122``) and
``adaptor::to_graph<Interface>`` (``adaptor_wiring.h:596-612``) are the same
function:

.. code-block:: text

   // service::impl_output                        // adaptor::to_graph
   endpoint = reference_output_path<Service>(p);  // adaptor_to_graph_path<Interface>(p)
   merge_resolution(p.resolution,
       w.service_implementation_stub_resolution(endpoint));        // identical
   w.register_service_implementation_stub(
       endpoint, "reference service");            // …, "adaptor"
   auto shared = reference_shared_output_source   // output_source
       <Service>(w, p);                           //   <Interface>(w, p)
   auto capture = capture_reference_service_      // capture_output
       output<Service, …>(w, out, shared, p);     //   <Interface>(w, out, shared, p)
   w.register_service_rank_anchor(
       reference_base_path<Service>(p), capture); // adaptor_to_graph_path<…>(p), capture

Likewise ``service::impl_input`` (``:1064-1102``) is the ``from_graph``
equivalent for the subscription and request/reply flavours. A reference service
has no input by construction, so it correctly has no ``from_graph``.

So the gap is narrower than "services lack from_graph/to_graph". It is:

1. **Naming and discoverability.** ``impl_input`` / ``impl_output`` are the same
   verbs under names that do not suggest the adaptor model. Propose
   ``service::from_graph`` / ``service::to_graph`` as the primary spelling, with
   the existing names retained as aliases so nothing breaks.
2. **A lazy single-interface registration that permits by-stub publishing.**
   ``impl_output`` is only reachable through ``register_services``, which wires
   **eagerly** (``service_wiring.h:1178-1208``, calling
   ``wire_service_graph_with_scope`` directly). ``register_reference_service`` is
   lazy but by-return only. The adaptor surface has both
   (``register_adaptor`` = lazy + by-stub; ``register_automatic_adaptor`` =
   lazy + by-return). Services need the missing quadrant: a lazy,
   single-interface, by-stub registration.

This also resolves difference 1 far better than the original draft. Instead of
forcing every migrated implementation to by-return, **services support both
publish styles**, and a manual source-only adaptor migrates by changing
``adaptor::to_graph<Iface>`` to ``service::to_graph<Service>`` — a rename, not a
body rewrite.

It resolves the review's P1 objection too. With both styles supported, each gets
the enforcement that fits it: a by-stub implementation is checked by the
required-endpoint scope (which it does satisfy, because it registers stubs), and
a by-return implementation is checked by returned-port validation. The
mismatch that made the original proposal unimplementable disappears.

A point in the other direction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The audit also found that a **generic source-only ``@adaptor`` is effectively
unusable in Python today**. ``_AdaptorClientStub._prepare_client_request``
(``_services.py:605-657``) infers resolution only from request ports
(``:634-641``) and never calls ``_registered_service_resolution``, so with zero
requests the resolution stays empty and ``_require_descriptor()`` raises
``"generic adaptor '…' must be specialized"`` (``:736-739``).
``_ServiceStub.__call__`` handles the zero-request case explicitly
(``:518-522``). Migration therefore *fixes* generic source-only interfaces
rather than regressing them.

Supporting evidence that the collapse is already half-done in the code:
``_bind_registered_impl`` (``_services.py:1293-1296``) computes

.. code-block:: python

   native_ports = (
       0 if stub is None or stub.flavour == "reference"
       or (stub.flavour == "adaptor" and expected_ports == 0) else 1
   )

— the source-only adaptor case is *already* folded onto the ``"reference"``
branch. Removing source-only adaptors makes that clause dead.

Incidental duplication to remove with it
----------------------------------------

The same audit found the two namespaces carry byte-for-byte copies of several
helpers. Removing the source-only case is the opportunity to share them:

* ``ServicePath`` (``service_wiring.h:28-33``) and ``AdaptorPath``
  (``adaptor_wiring.h:26-31``) are field-identical structs; ``service::path``
  and ``adaptor::path`` both forward to ``wiring_path_detail::typed_path_value``.
* ``bind_schema_resolution`` / ``resolved_schema_meta``
  (``service_wiring.h:220-255`` vs ``adaptor_wiring.h:96-131``) differ only in an
  error string.
* ``is_path_scalar`` / ``has_path_scalar`` / ``implementation_accepts_path``
  (``service_wiring.h:322-331, 390-401`` vs ``adaptor_wiring.h:165-187``) and
  ``path_key_value`` (``:590-594`` vs ``:265-268``) are identical.
* The erased node factories are *already* shared — ``shared_output_source_node``
  and ``shared_output_capture_node`` (``service_runtime.cpp:136-178``) serve both
  families today, which is the strongest evidence that the template layer's
  split is accidental.
* ``capture_reference_service_output``'s ``Impl`` template parameter
  (``service_wiring.h:875``) is unused and can go.

The role marker typeids can also merge: the path key already carries the family
prefix, so ``reference_output_source_marker`` and
``adaptor::detail::output_source_marker`` cannot collide even if unified.

Proposed Python contract
------------------------

``@adaptor`` on a function with a return annotation and no time-series
parameters is deprecated, then rejected, with the error naming
``@reference_service`` as the replacement. ``@adaptor_impl`` for such an
interface likewise becomes ``@service_impl``, and ``register_adaptor`` becomes
``register_service``.

.. code-block:: python

   # before
   @adaptor
   def tick_feed(path: str) -> TS[int]: ...

   @adaptor_impl(interfaces=tick_feed)
   def tick_feed_impl(path: str) -> TS[int]: ...

   register_adaptor("feed", tick_feed_impl)

   # after
   @reference_service
   def tick_feed(path: str) -> TS[int]: ...

   @service_impl(interfaces=tick_feed)
   def tick_feed_impl(path: str) -> TS[int]: ...

   register_service("feed", tick_feed_impl)

``_FLAVOUR_TS_ARITY`` (``python/hgraph/_wiring/_services.py:1117-1123``) already
assigns arity 0 to both ``"reference"`` and ``"adaptor"``; the ``"adaptor"``
entry keeps that arity for the sink-only and duplex shapes, which declare a
time-series parameter.

Runtime representation and dispatch
-----------------------------------

``ServiceFlavour::Adaptor`` remains, since duplex and sink-only adaptors keep
it. What changes is that a descriptor with ``flavour == Adaptor`` and
``input_schema == nullptr`` becomes invalid and is rejected at
``intern_service_descriptor``. A source-only interface is interned as
``ServiceFlavour::Reference`` instead.

The bridge enforcement point is ``python/py_state_services.cpp:304-308``, where
the ``"adaptor"`` flavour currently accepts an absent request schema:

.. code-block:: cpp

   else if (flavour == "adaptor")
   {
       descriptor.flavour = ServiceFlavour::Adaptor;
       if (request.has_value()) { descriptor.input_schema = request->meta; }   // adaptor input
       if (output.has_value()) { descriptor.output_schema = output->meta; }
   }

An absent ``request`` becomes an error naming ``@reference_service``. The
remaining ``ServiceFlavour::Adaptor`` switch arms
(``service_runtime.cpp:624, 637, 671, 730, 756, 785``;
``py_state_services.cpp:271, 344, 366``) keep their adaptor behaviour for the
duplex and sink-only shapes and need no new ``Reference`` case.

The path grammar converges accordingly: what was
``adaptor://<user-path>/<name>/to_graph`` becomes ``ref_svc://<user-path>/<name>``.
The relay node identities become the reference-service markers. No node
builder, scheduling rule, or storage layout changes — the same
``make_shared_output_source_node`` and ``make_shared_output_capture_node`` pair
runs, with the same ``add_same_cycle_pair`` contract.

In-tree inventory
-----------------

The whole migration is five declarations, **none of them production code**:

.. list-table::
   :header-rows: 1
   :widths: 45 15 40

   * - Declaration
     - Kind
     - Note
   * - ``tests/cpp/test_adaptor_wiring.cpp:110`` ``SourceOnlyAdaptor``
     - test
     - straight migration
   * - ``tests/cpp/test_adaptor_wiring.cpp:125`` ``TypedSourceAdaptor``
     - test
     - scalar-qualified path case
   * - ``tests/cpp/test_adaptor_wiring.cpp:216`` ``MultiOutAdaptor``
     - test
     - **multi-interface** — see below
   * - ``python/tests/test_hgraph_api.py:1222`` ``source``
     - test
     - straight migration
   * - ``python/tests/test_hgraph_api.py:1254`` ``fallback_source``
     - test
     - exercises default-fallback registration

Every ``@adaptor`` in ``python/hgraph/adaptors/`` (tornado HTTP and WebSocket
servers, perspective ``publish_table`` / ``publish_table_editable``) is duplex or
sink-only, as is every other C++ adaptor interface in the tree. Nothing shipped
declares a source-only adaptor.

The one genuine casualty
~~~~~~~~~~~~~~~~~~~~~~~~

``MultiOutAdaptor`` is half of the multi-interface example that
``docs/source/developer_guide/services.rst`` documents as a *feature*:

.. code-block:: cpp

   adaptor::register_adaptors<MultiAdaptorImpl, MultiInAdaptor, MultiOutAdaptor>(w, custom);
   // "one implementation can serve multiple interfaces — e.g. a sink-only
   //  interface in, a source-only interface out"

If source-only adaptors go, that pattern can no longer be expressed as a single
adaptor registration. The implementation would register a sink-only adaptor for
the input half **and** a reference service for the output half — two
registrations for what is currently one, and the atomicity that
``register_adaptors`` provides across the interface set is lost.

Three ways out, for review:

* **Accept it.** Document the sink-adaptor-plus-reference-service pairing as the
  replacement. Simplest, and the atomicity matters less now that
  implementations are inlined and materialised on demand.
* **Keep source-only adaptors solely as members of a multi-interface set.**
  Preserves the example, but retains exactly the duplication this RFC removes
  and reintroduces the ``wire<>`` ambiguity for those types.
* **Extend ``register_services`` to mix flavours**, so one implementation can
  atomically own a sink adaptor and a reference service. Cleanest conceptually,
  largest scope, and arguably its own RFC.

This is the main open decision in this proposal; the recommendation is the
first option.

Compatibility and migration
---------------------------

This is a breaking change to a public wiring surface, taken deliberately under
the hgraph 1.0 clean-break milestone (:doc:`rfc_0005_hgraph_1_0_api`). The
in-tree cost is five test declarations; the external cost depends on downstream
adopters, which the removal stage must confirm.

Consequences to plan for:

* **Registry keys change** for migrated interfaces (``adaptor://…/to_graph`` →
  ``ref_svc://…``). This is visible in node labels
  (``shared_output_source:<path>``), registry snapshots, inspector output, and
  any test asserting those strings.
* **``GlobalState`` keys** derived from adaptor paths by implementations that
  stash a sender under ``f"…adaptor://…"`` must be re-derived. Implementation
  bodies that key off the *user* path rather than the full path are unaffected.
* **Implementations that publish manually** lose their ``adaptor::to_graph``
  call and return the port instead (difference 1 above). Automatic adaptors need
  no body change.
* **Generic source-only interfaces** see their client port schema change from
  ``REF<T>`` to the resolved ``T`` (difference 4). Concrete interfaces are
  unaffected.
* **Implementations with a mis-declared output** start failing at wiring time
  rather than silently building a mismatched capture (difference 3).
* **Multi-interface registrations** that mix a source-only adaptor with other
  adaptor interfaces cannot be migrated piecemeal: ``register_adaptors``
  registers one deferred candidate over all base paths
  (``adaptor_wiring.h:554-566``) while ``register_services`` wires eagerly
  (``service_wiring.h:1206-1207``). Such a case must move wholesale or keep an
  input. The inventory below determines whether any exist.
* **Deprecation window.** Stage one emits a deprecation warning from the
  source-only ``@adaptor`` / ``adaptor::interface`` path while both work; stage
  two removes it. The two stages are separate PRs so downstream code has a
  release to migrate in.

Performance and memory
----------------------

No per-tick path changes. The relay nodes, their storage, their scheduling, and
their binding are identical before and after; only which wiring helper
constructs them changes. Net effect on the build is a reduction: one pair of
template instantiations per interface instead of two, and roughly 120 lines of
duplicated header code removed.

Alternatives considered
-----------------------

Disambiguate the concepts and keep both constructs
   Add ``!std::derived_from<Service, adaptor::interface>`` to
   ``reference_service_interface``. This fixes the compile error in a few lines
   and changes no behaviour. Rejected as the primary answer because it
   entrenches the duplication the error is a symptom of — precisely the
   "two ways to do one thing" guardrail. It is, however, the correct **interim**
   fix if this RFC is not accepted, and it is what
   ``tests/cpp/test_service_push_sources.cpp`` (PR #257) currently works around
   with a duplex adaptor.

Fold reference services onto the adaptor machinery instead
   Rejected, but less one-sidedly than first drafted. Adaptors generalise to
   duplex and sink-only, and they own two capabilities reference services lack
   entirely (catch-all registration and client scalar options — see
   "Capabilities to lift onto services"). ``default_fallback`` is symmetric, not a
   reference-service advantage. The case for the reference service as primitive
   rests on it being the narrower, older notion — "a value published by one
   producer" — and on its stricter checking; a source-only adaptor is a
   reference service whose implementation happens to talk to the outside world,
   and the reverse framing does not hold for the *source-only* shape.

Introduce a third shared "boundary relay" primitive both build on
   Rejected. It removes the duplicated code but adds a concept to the
   vocabulary and leaves two public spellings of one idea, so the surface does
   not actually shrink.

Internal de-duplication only, both public surfaces retained
   Rejected for the same reason: it hides the duplication rather than resolving
   it, and users still face two names for one construct.

Unresolved questions
--------------------

**Decided (Howard, 2026-08-02):**

* Client scalar options and time-series registration configuration are **lifted
  onto the service surface**, not traded away — "the best of both worlds, not a
  restriction of one to enforce the other". The ``from_graph`` / ``to_graph``
  usage model is lifted with them. Recorded as capabilities A, B and D above.
* Step 4 **adds no new registration verb**: ``register_services`` becomes lazy
  and serves the single-interface by-stub case, and no singular
  ``service::register_service`` alias is introduced.
* The C++-template-only fail-fast check for a never-published *unused*
  registration is **dropped**; ``test_service_wiring.cpp:2128`` gains a client.

Still open:

* **The multi-interface case.** With services gaining ``to_graph`` and a lazy
  by-stub registration, the documented sink-in/source-out example becomes
  expressible again *if* one implementation may span an adaptor and a service.
  The erased ``register_multi_service_impl`` already spans flavours
  (``service_runtime.cpp:762-788``); only the template surface is statically
  disjoint (``register_adaptors`` requires ``adaptor_interface``,
  ``register_services`` requires ``service_interface``). Relaxing that to a
  mixed-flavour group would preserve the example and the registration
  atomicity. Recommendation revised: **relax the template constraint** rather
  than accept the loss, since it follows the same lift-don't-restrict principle.
  It does enlarge the RFC.
* Should ``@service_impl(interfaces=())`` gain catch-all support, so the
  capability is not adaptor-exclusive? Not required by this RFC — catch-all
  survives with the adaptor family — but it is the remaining asymmetry once
  source-only is gone, and the same principle would say yes.
* Should the deprecation stage warn at wiring time (every graph build) or once
  per interface at decoration/registration? Once per interface is quieter but
  easier to miss.
* Does any downstream repository declare a source-only adaptor? Nothing in this
  tree does outside tests, but the removal stage should not land until that is
  confirmed.
* Should ``intern_service_descriptor`` reject ``Adaptor`` + null
  ``input_schema`` outright, or silently re-intern it as ``Reference``? Rejecting
  is clearer; re-interning is kinder to code that builds descriptors
  programmatically through the bridge.
* Given that nothing in production uses the form, is a deprecation window
  warranted at all, or should the removal be a single change under the 1.0
  clean break?

Acceptance criteria and test plan
---------------------------------

Stage 1 — lift (additive; no existing behaviour changes)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Service clients accept wiring-time scalar options that reach the registered
  implementation, for **all three** service flavours, with the same
  cross-client agreement diagnostic adaptor clients get today. A test asserts
  two clients disagreeing on one option fails with the flavour named correctly.
* ``register_service`` accepts time-series registration configuration and wires
  it as implementation inputs, matching ``register_adaptor``. The bridge's
  ``register_service_impl`` gains the ``inputs`` parameter.
* ``service::from_graph`` / ``service::to_graph`` exist as the primary spelling,
  with ``impl_input`` / ``impl_output`` retained as aliases; existing callers
  keep compiling unchanged.
* A **lazy, single-interface, by-stub** service registration exists — the
  missing quadrant against ``register_adaptor`` / ``register_automatic_adaptor``
  — and an implementation registered that way is checked by the
  required-endpoint scope, failing with the existing "did not wire required
  stub" diagnostic when it publishes nothing.
* A by-return implementation continues to be checked by returned-port
  validation. Both publish styles are covered for the same service.
* Every existing service and adaptor test still passes with no edits: stage 1
  removes nothing.

Stage 2 — collapse (breaking)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``adaptor::detail::adaptor_interface`` requires an input schema; a source-only
  interface fails to satisfy it with a diagnostic naming ``reference_service``.
* ``reference_service_interface`` and ``adaptor_interface`` are provably
  disjoint: a test translation unit including **both** ``service_wiring.h`` and
  ``adaptor_wiring.h`` wires a duplex adaptor, a sink-only adaptor and a
  reference service with no ambiguity. This is the regression test for the
  compile error above, and it lets
  ``tests/cpp/test_service_push_sources.cpp`` (PR #257) drop its duplex
  workaround.
* Every in-tree source-only adaptor is migrated to a reference service with
  behaviour unchanged: same values, same cycle counts, same rank order.
* A reference-service implementation whose output does not match the interface
  is rejected at wiring time for a **concrete** schema (already true) **and for
  a generic one** (difference 3, the new work). The generic mismatch test is
  mandatory: a concrete-only test would pass while the generic template path
  stayed unchecked.
* A **generic** migrated interface returns the resolved value schema rather than
  ``REF<T>`` on the template client path, matching the erased runtime
  (difference 4), with a test asserting the port schema.
* The shared path/resolution helpers exist once, and both surfaces use them.
* A **generic** source-only interface, which cannot currently be used as an
  adaptor from Python at all (``"generic adaptor '…' must be specialized"``),
  works as a reference service.
* No catch-all implementation is left expecting to serve a migrated client:
  the tornado HTTP and WebSocket catch-alls
  (``http_server_adaptor.py:453``, ``websocket_server_adaptor.py:437``) still
  discover their endpoints after the migration.
* Docs updated: ``services.rst:313-318, 331-334, 365-382`` and
  ``user_guide/authoring_graphs_cpp.rst:886-893, 1090-1093`` all currently
  describe source-only adaptors as supported.
* ``tests/cpp/test_adaptor_wiring.cpp`` keeps its duplex, sink-only,
  multi-interface, automatic-registration, scalar-qualified-path and generic
  cases; its source-only cases move to the service suite.
* A source-only ``@adaptor`` in Python raises with a message naming
  ``@reference_service``.
* A descriptor with ``flavour == Adaptor`` and no ``input_schema`` is rejected
  at ``intern_service_descriptor``.
* The full native suite and the non-WIP Python compatibility suites pass on
  macOS and Linux, including ``python/tests/ported`` (which is not part of the
  default ``ctest`` run).
* ``docs/source/developer_guide/services.rst`` records the collapse; the
  adaptor section states that adaptors are duplex or sink-only and points the
  source-only case at reference services.

Implementation plan
-------------------

Nine PR-sized steps. Steps 1-6 are stage 1 (additive, each independently
mergeable and independently useful); steps 7-9 are stage 2 (breaking). Planning
the work sharpened three estimates in this RFC — recorded inline below and
summarised under "What planning changed".

**The unifying insight for the C++/erased work:** ``materialize_adaptor_impl``
(``service_runtime.cpp:956-1002``) is already the shape services need. It
carries an ``AdaptorImplMode`` (``service_runtime.h:131-135``) whose
``Automatic`` arm publishes by return and whose ``Manual`` arm publishes by
stub, an ``implementation_inputs`` span, a required-endpoint scope, and arity
validation for both modes. ``register_reference_service_impl`` is that same
function with mode fixed to ``Automatic``, no inputs, and no scope. Most of
stage 1 is generalising one materializer rather than writing new machinery.

Step 1 — service client scalar options (Python only)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Smaller than the RFC first implied: the mechanism has **no C++ component at
all** (``_ADAPTOR_CLIENT_CONFIGS`` is a Python dict; grepping ``src/``,
``include/`` and ``python/*.cpp`` for it returns nothing), and the
implementation-consumption half already works for services —
``register_service(path, impl, **kwargs)`` (``_services.py:1809``) already
forwards scalar config, and ``_bind_registered_impl`` (``:1380-1401``) already
merges it with client config generically.

So the whole step is the **client-recording half**:

* ``_ServiceStub._bind_call`` (``:477-491``) already builds the ``bound``
  signature object and then discards everything except path and requests —
  return it, or record inside it.
* Call the existing ``_record_adaptor_client_config(stub, path, bound)`` from
  ``_ServiceStub.__call__``. No changes to the helper: its key already includes
  ``stub.flavour`` and its diagnostic already interpolates it.
* Rename ``_record_adaptor_client_config`` / ``_adaptor_client_config`` /
  ``_ADAPTOR_CLIENT_CONFIGS`` to flavour-neutral names.

Tests: a reference, a subscription and a request/reply service each taking a
scalar option; two clients agreeing; two clients disagreeing (expect the
existing diagnostic with the right flavour name); client option conflicting
with a registration option (expect the existing ``_bind_registered_impl``
error).

Step 2 — time-series registration configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cross-language, mirroring the adaptor path at every layer:

* C++ erased: add ``std::span<const WiringPortRef> implementation_inputs`` to
  ``register_reference_service_impl`` / ``register_subscription_service_impl`` /
  ``register_request_reply_service_impl``, defaulted empty, threaded into
  ``wire_impl`` exactly as ``materialize_adaptor_impl`` does, with the same
  arity check against ``WiredFn::arity``.
* Bridge: ``register_service_impl`` (``py_state_services.cpp:349-371``) gains
  ``inputs``, matching ``register_adaptor_impl`` (``:464-476``).
* Python: generalise ``_adaptor_registration_inputs`` (``:1075-1090``) past its
  ``manual_adaptor`` gate — for services the gate is "the implementation
  declares time-series parameters the interface does not supply" — and forward
  from ``_register_resolved_service`` (``:1785-1806``).

Tests: a service implementation taking a wired TS input at registration;
arity mismatch rejected; the existing adaptor cases unchanged.

Step 3 — ``service::from_graph`` / ``service::to_graph``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pure naming. ``service::to_graph<Service>`` is ``impl_output`` and
``service::from_graph<Service>`` is ``impl_input``; add the new names as the
primary spelling and keep the old ones as aliases. Python gains the same
spelling over ``impl_output`` / ``set_service_output``. No behaviour changes,
so the existing service tests are the regression suite.

Step 4 — the missing registration quadrant (make ``register_services`` lazy)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Add a **lazy, single-interface, by-stub** service registration — the
``register_adaptor`` analogue. Concretely, give the service materializers the
``Manual``/``Automatic`` mode that adaptors already have, and open a
required-endpoint scope in the ``Manual`` arm so a non-publishing
implementation fails with the existing "did not wire required stub"
diagnostic.

Generalise ``AdaptorImplMode`` to a flavour-neutral ``ServiceImplMode`` (or
equivalent) rather than adding a second enum.

*Naming — decided (Howard, 2026-08-02): add no verb at all.* The by-stub
quadrant already
exists on the template surface. ``register_services<Impl, Services...>``
(``service_wiring.h:1178-1208``) is by-stub and scope-enforced, and it already
accepts a **single** interface — ``test_service_wiring.cpp:1543`` uses
``register_services<MissingMultiServiceOutputImpl, AddOneService>`` with one
service today. Its only defect against the adaptor equivalent is that it wires
**eagerly** while ``register_adaptors`` is lazy.

Worse, it is eager only on the *template* surface: the erased
``register_multi_service_impl`` registers a candidate
(``service_runtime.cpp:854-861``), so a Python multi-service implementation is
already lazy. The template path diverges from its own erased counterpart, with
no rationale comment — evidence that the eagerness is incidental rather than
designed.

So step 4 becomes "make ``register_services`` lazy" rather than "add a verb":
register a candidate over the interfaces' base paths and move the
``register_built_service_path`` calls inside the materializer, exactly as
``register_reference_service`` does.

*The one real cost.* ``test_service_wiring.cpp:2128`` asserts that
``build_graph<MissingMultiServiceStubGraph>()`` throws — a ``register_services``
implementation that never publishes, **with no client**. Eager wiring is what
catches it. Once lazy, an unrequested candidate is never materialized, so its
scope never runs and nothing throws; the test must add a client, after which the
same enforcement fires as before.

**Accepted (Howard, 2026-08-02): take the drop**, and add a client to that
test. The check should not decide the design: it is C++-template-only (Python
never had it), it contradicts the documented "implementation candidates
materialize only on demand" contract that ``test_service_wiring.cpp:1793`` pins
for reference services, and it forces an unused implementation to be built. If
fail-fast validation of *unused* registrations is wanted later, it returns as a
separate opt-in facility across all flavours — not as a property preserved by
keeping one verb eager.

*Also decided — no singular ``service::register_service`` alias.* It would
read symmetrically with ``adaptor::register_adaptor``, but Python's
``register_service(path, impl)`` is the *general* registration verb covering the
by-return single-interface case. The same name would mean something different in
each language. Keep the plural; it reads correctly for one interface ("register
the services this implementation provides").

Step 5 — close the generic output-schema hole
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Difference 3. In the non-concrete branch of ``wire_service_impl``
(``service_wiring.h:403-429``), compare the implementation's resolved meta
against ``resolved_schema_meta<OutputSchema>`` before returning. Independent of
the collapse and worth landing on its own: it is a real hole in the current
reference-service surface, not only a migration prerequisite.

Test: a generic reference service whose implementation returns the wrong
resolved type is rejected. This test must fail before the change.

Step 6 — deduplicate the shared helpers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mechanical, no behaviour change: one ``ServicePath``/``AdaptorPath``, one
``bind_schema_resolution`` / ``resolved_schema_meta`` / ``is_path_scalar`` /
``implementation_accepts_path`` / ``path_key_value``. Drop the unused ``Impl``
parameter on ``capture_reference_service_output``. Best done after steps 1-5 so
it rebases over settled code.

Step 7 — deprecate source-only adaptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Warn from the source-only ``@adaptor`` and ``adaptor::interface`` paths, naming
``@reference_service`` / ``register_reference_service``. Both spellings still
work. Migrate the five in-tree declarations to services in the same change, so
the tree is warning-free and the migration is demonstrated.

Step 8 — mixed-flavour multi-interface groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Relax ``register_services`` / ``register_adaptors`` so one implementation may
span an adaptor and a service, preserving the documented sink-in/source-out
example. The erased ``register_multi_service_impl`` already spans flavours
(``service_runtime.cpp:762-788``); only the template ``static_assert`` s are
disjoint. **Gated on the open decision** — if that is declined, this step is
replaced by documenting the two-registration replacement.

Step 9 — remove source-only adaptors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Narrow ``adaptor_interface`` to require an input; reject ``Adaptor`` with a null
``input_schema`` at ``intern_service_descriptor`` and at the bridge
(``py_state_services.cpp:304-308``); delete the source-only overloads; update
``services.rst`` and ``authoring_graphs_cpp.rst``. Add the disjointness
regression test — one translation unit including both headers — and drop the
duplex workaround in ``tests/cpp/test_service_push_sources.cpp``.

What planning changed
~~~~~~~~~~~~~~~~~~~~~

Three estimates in this RFC were revised while planning, all downward:

* **Capability A is Python-only and half-done.** It has no C++ component, and
  implementation-side scalar config already works for services. Only client
  recording is missing.
* **The "missing quadrant" already exists in another guise.**
  ``AdaptorImplMode::Manual`` versus ``Automatic`` is exactly the by-stub versus
  by-return distinction services need; step 4 generalises an enum and a
  materializer rather than designing a mechanism.
* **Step 5 is independent.** Closing the generic output-schema hole stands on
  its own merits and need not wait for, or be justified by, the collapse.

The overall shape holds: the lift is smaller than "materially larger RFC"
suggested, and most of it is generalising code that already exists.

Implementation status
---------------------

Not started. This RFC is the first commit on its branch, per
:doc:`rfc_0000` workflow step 1; the plan above is the proposed sequencing.

References
----------

* :doc:`rfc_0000` — RFC process.
* :doc:`rfc_0005_hgraph_1_0_api` — the 1.0 clean-break milestone this breaking
  change is taken under.
* ``docs/source/developer_guide/services.rst`` — the authoritative design record
  for the boundary layer, including "Implementations are inlined, not nested".
