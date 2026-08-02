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
(publishing contract, scope enforcement, output-schema checking, and generic
client schema). Three of the four are places where the reference-service
behaviour is the stricter and better one, so the collapse is a net improvement
rather than a rename. They are enumerated and resolved below.

This RFC proposes collapsing them. The reference service is the primitive; the
source-only adaptor form is deprecated and then removed. The adaptor surface
remains for the cases that genuinely differ from any service flavour — duplex
(input and output) and sink-only (input, no output).

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

In scope: the **source-only** case — an interface declaring an output and no
input.

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
Python change follows the C++ one rather than defining a second contract. No
new runtime node type, ops-table slot, or storage shape is introduced: this RFC
removes code paths, it does not add any.

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

*Proposed:* migrate to by-return. An automatic source-only adaptor migrates with
no body change; a manual one drops its ``to_graph`` call and returns the port
instead. The RFC should say so explicitly because it is the only implementation
edit the migration requires.

**2. Implementation-scope enforcement is lost.** The adaptor path opens a
``service_implementation_scope`` with required endpoints
(``adaptor_wiring.h:451``, ``:507-508``), so ``end_service_implementation``
(``graph_wiring.cpp:2221-2240``) fails an implementation that never published.
``register_reference_service`` opens no scope, so a reference-service
implementation that silently fails to produce is not caught the same way.

*Proposed:* give ``register_reference_service`` the same scope treatment —
``service_wiring.h:996-1006`` already has ``wire_service_graph_with_scope``, it
is simply not used here. This is a strict improvement to the reference-service
surface and should land as part of the collapse rather than being lost with it.

**3. Output-schema conformance is checked on only one side.** The reference path
runs ``describe_service_output`` (``service_runtime.cpp:190-209``) and
``Port::as<OutputSchema>()``, which reject an implementation whose output does
not match the interface. The adaptor path only null-checks (``:897-900``,
``:995-999``) and builds the capture from the *implementation's* meta while the
source uses the *interface's* meta.

*Proposed:* the collapse adopts the checked behaviour. Any in-tree source-only
adaptor relying on the unchecked path is by definition mis-declared and is
fixed as part of migration.

**4. Generic interfaces return a different schema.** For a non-concrete output
schema, the reference client patches the port schema down to the resolved
value meta (``service_wiring.h:1023-1025``); the adaptor client leaves the
``REF`` meta (``adaptor_wiring.h:440``). Both erased runtime forms patch
(``service_runtime.cpp:268``, ``:948``), so this asymmetry exists only on the
template path.

*Proposed:* adopt the patching behaviour (the erased runtime already agrees with
it). A generic source-only adaptor's client port therefore changes observable
schema — this needs an explicit migration test, not just a compile check.

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
   ``tests/cpp/test_service_push_sources.cpp`` currently works around with a
   duplex adaptor.

Fold reference services onto the adaptor machinery instead
   Rejected. Adaptors generalise to duplex and sink-only, but reference services
   carry ``default_fallback`` and catch-all registration and are the older,
   more primitive notion — "a value published by one producer". A source-only
   adaptor is a reference service whose implementation happens to talk to the
   outside world; the reverse framing does not hold.

Introduce a third shared "boundary relay" primitive both build on
   Rejected. It removes the duplicated code but adds a concept to the
   vocabulary and leaves two public spellings of one idea, so the surface does
   not actually shrink.

Internal de-duplication only, both public surfaces retained
   Rejected for the same reason: it hides the duplication rather than resolving
   it, and users still face two names for one construct.

Unresolved questions
--------------------

* **The multi-interface case above is the main open decision.** Recommendation:
  accept the loss and document the replacement pairing.
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

* ``adaptor::detail::adaptor_interface`` requires an input schema; a source-only
  interface fails to satisfy it with a diagnostic naming ``reference_service``.
* ``reference_service_interface`` and ``adaptor_interface`` are provably
  disjoint: a test translation unit including **both** ``service_wiring.h`` and
  ``adaptor_wiring.h`` wires a duplex adaptor, a sink-only adaptor and a
  reference service with no ambiguity. This is the regression test for the
  compile error above, and it lets
  ``tests/cpp/test_service_push_sources.cpp`` drop its duplex workaround.
* Every in-tree source-only adaptor is migrated to a reference service with
  behaviour unchanged: same values, same cycle counts, same rank order.
* ``register_reference_service`` opens an implementation scope and fails an
  implementation that publishes nothing (difference 2), with a test that pins
  the diagnostic.
* A reference-service implementation whose output does not match the interface
  is rejected at wiring time (difference 3).
* A **generic** migrated interface returns the resolved value schema rather than
  ``REF<T>`` on the template client path, matching the erased runtime
  (difference 4), with a test asserting the port schema.
* The shared path/resolution helpers exist once, and both surfaces use them.
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

Implementation status
---------------------

Not started. This RFC is the first commit on its branch, per
:doc:`rfc_0000` workflow step 1.

References
----------

* :doc:`rfc_0000` — RFC process.
* :doc:`rfc_0005_hgraph_1_0_api` — the 1.0 clean-break milestone this breaking
  change is taken under.
* ``docs/source/developer_guide/services.rst`` — the authoritative design record
  for the boundary layer, including "Implementations are inlined, not nested".
