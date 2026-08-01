Linking Strategies
==================

The terms structural, peered, sampled, active, and published follow the
definitions in :doc:`../binding_vocabulary`.

Time-series instances rarely operate in isolation. A node's input
borrows another node's output; a ``REF`` value resolves into a target
output; a nested graph's output is presented as a parent's output
without copying the data. The runtime distinguishes three flavours of
"this position delegates to another position" — each with its own
state type, lifetime contract, and notification shape — and uses them
to support binding, unbinding, and rebinding across all of these
cases:

================  ===================  =================================================
Strategy          Direction            Used for
================  ===================  =================================================
TargetLink        input → output       Ordinary input-to-output binding (the common case)
RefLink           output → output      Dereferencing a REF inside an output alternative
ForwardingLink    output → output      Aliasing a child output as a parent output
                                       (nested-graph boundary export)
================  ===================  =================================================

Each link points at a memory-stable target — see *Time-Series Plans
and Ops > Memory Stability Invariant*. Link targets are framework-owned
lifecycle relationships: a link must be unbound before its target
storage is destroyed, or the target must outlive the link. Violating
that ordering is a runtime lifecycle bug rather than a condition hidden
by best-effort invalidation.

TargetLink
----------

TargetLink is the input-side storage type for a peered terminal inside
a ``TSInput`` data plan. Peered terminals are declared by the generic
``TSEndpointSchema`` annotation tree; the ``TSInputPlanFactory``
validates that the root is a non-peered input bundle and that every
peered terminal matches the schema position it annotates. The input
terminal carries:

- compact in-plan tracking with the input-local ``last_modified_time``
  used to bubble changes through non-peered input prefixes;
- inline target-link state, because the normal runtime state is a bound
  link rather than an empty terminal;
- in that state, the borrowed output handle, the internal target
  observer registered with the bound output ``TSData`` observer set, and
  the root pointer for the active descendant trie.

The storage plan should not embed a full output view or an eager active
collection. The target binding is a borrowed ``TSOutputHandle`` plus
observer identity; evaluation time belongs on endpoint views, not in the
link's stored state. Endpoint views recreate transient ``TSDataView``
cursors from the stored output handle when they need target behaviour.

The wiring-time schema selects one of two target-link storage plans. Scalar,
fixed, window, reference, and signal terminals allocate only the common
tracking and binding state plus a non-null no-op structural ops pointer.
``TSS`` and ``TSD`` terminals allocate the structural representation, which
adds a compact ``SlotObserverList`` and a lazy sampled-transition record. The
semantic target-link code dispatches through the selected passive ops table;
it does not inspect a representation tag or schema kind on the runtime path.

The structural observer list has a narrower purpose than the target-modified
and scheduling observers below. It is populated only when another keyed-slot
consumer observes the target link itself, as happens in forwarding and proxy
chains. The link retains those observer identities across an unbound period,
subscribes them directly to the current output key store on bind/rebind, and
unsubscribes or sends ``on_clear`` before the borrowed target disappears. Its
empty and one-observer forms occupy one tagged pointer; only two or more
observers allocate a spill list.

A link-local delegation hub was measured but is not used. In the representative
workloads and complete native suite every exercised forwarding list had one
observer, so direct and delegated registration retained the same number of
pointers. Delegation would only add a callback hop in that measured shape.

A subtle point: a peered input terminal carries **two** notification paths:

- **Target-modified path.** Subscribed to the target's modification
  notifier. When the target ticks, this path marks the link state
  modified and propagates the tick up through the parent chain
  (non-peered collections above the link).
- **Scheduling path.** Forwards scheduling
  notifications to the owning node, but does *not* mark the link
  state itself modified. Active target descendants are tracked in the
  link's active trie and share the link's scheduling notifier. This
  preserves descendant path identity without storing a path-to-target map
  in the TargetLink state.

Under a peered boundary, scheduling and modification flow through
different identities even though both ultimately reach the same owning
node.

RefLink
-------

RefLink is the output-side dereference of a ``REF`` value. It is
created as the payload of an output alternative when an input expects a
non-``REF`` shape but the output position being bound is a ``REF``
shape. It is not part of the normal input binding path and it is not a
public way for user code to dereference a ``TimeSeriesReference``.

A ``RefLinkState`` is logically split into three pieces:

- the **REF source subscriber** — observes the output position where the
  ``TimeSeriesReference`` value lives. This subscriber attaches a
  notifier to the source output and reacts to every source tick.
- the **reference-bound target endpoint** — exposes the output shape
  currently named by the reference value. It supports bind, unbind, and
  rebind, but its binding input is ``TimeSeriesReference`` rather than a
  wiring-time ``TSOutputHandle``.
- the **downstream attachments** — one attachment per consuming input
  notification identity. Each attachment records the active descendant
  paths requested by that input under this dereference boundary.

The current C++ implementation names the output-owned state
``RefLinkAlternativeState``. It implements the source subscriber and
reference-bound endpoint pieces. Downstream attachment replay is still a
separate follow-up for the active/passive input binding layer; the
target endpoint already uses TargetLink mechanics, so descendant target
ticks propagate through the exposed alternative.

The source subscriber's job is to drive the target endpoint. When the
``REF`` source value changes, the subscriber reads the new
``TimeSeriesReference`` and asks the target endpoint to rebind. The
target endpoint owns the mechanics that are otherwise familiar from
``TargetLink``: target binding data, target modification subscription,
unbind cleanup, and replay of active subscriptions after a rebind.

The target endpoint is not necessarily a single link. A
``TimeSeriesReference`` can describe a non-peered structure whose
children each reference different outputs. In that case the target
endpoint mirrors the requested exposed schema and holds child target
state at the corresponding slots, much like a ``TSInput`` plan holds
non-peered prefixes above peered target links. The exposed schema is
still fixed by the alternative request; the reference value supplies the
current target outputs inside that shape.

For this first pass, static ``TSB`` and fixed-size ``TSL`` prefixes are
expanded into non-peered endpoint-plan storage and all other shapes are
TargetLink leaves. Dynamic keyed non-peered reference expansion needs
the same explicit slot/key lifecycle work as other dynamic structures
before it should be enabled.

For example, a ``RefLink`` exposing:

.. code-block:: text

   TSB[{bid: TS[float], ask: TS[float]}]

may be rebound from a non-peered ``TimeSeriesReference`` that maps
``bid`` to one output and ``ask`` to another. The link still exposes one
coherent bundle output alternative, but its target endpoint contains two
child target bindings.

State shape
~~~~~~~~~~~

The state should be organised around handles and binding data, not
transient views:

.. code-block:: text

   RefLinkState
     source
       binding data for the REF source position
       notifier subscribed to the REF source

     target endpoint
       exposed schema requested by the alternative
       current target state for that exposed schema

     attachments
       input-notification-id -> RefLinkAttachment

   RefLinkAttachment
     forwarding scheduling notifier for that input
     active trie contributed by that input

The target endpoint is itself recursive:

.. code-block:: text

   RefLinkTargetNode
     expected exposed schema at this node
     current state:
       unbound
       bound output binding data
       structural child target nodes

For a scalar reference this tree has one node. For a non-peered
reference to a bundle or fixed list, the target node owns child nodes
that mirror the requested exposed schema. Dynamic keyed structures need
explicit slot/key lifecycle rules before recursive non-peered reference
binding should be enabled for them.

Binding a reference value
~~~~~~~~~~~~~~~~~~~~~~~~~

The target endpoint is rebound from a ``TimeSeriesReference`` value.
The exposed schema is fixed by the alternative request and does not
change when the reference changes.

Empty reference
    The target endpoint unbinds its current target state. Downstream
    attachments and their active tries remain attached to the
    ``RefLink`` so they can be replayed if a later reference value binds
    to a real target.

Single-output reference
    The target endpoint asks the referenced output for binding data
    matching the endpoint's expected exposed schema. The referenced
    output may return canonical binding data or an output alternative.
    The endpoint installs that binding data, subscribes to target
    modification notifications, and replays every downstream active trie
    against the new target.

Non-peered reference
    The target endpoint must be structural at the same level. The
    reference's child entries are applied to the corresponding child
    target nodes by field order for ``TSB`` and by index for fixed
    ``TSL``. Each child then follows the same empty, single-output, or
    non-peered binding rule. The exposed parent remains one coherent
    output alternative even though its children may point at different
    outputs.

If a reference value cannot satisfy the fixed exposed schema, binding
must fail loudly. A malformed reference should not be treated as "just
unbound", because that hides schema and lifecycle bugs.

Nested reference alternatives
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A ``RefLink`` owns one dereference boundary. It does not recursively own
all deeper alternatives below the target output. If the target output
itself has reference markers that differ from the requested exposed
shape, the target endpoint asks that output for the required binding
data and lets that output's alternative store create its own
``RefLink``.

For example, a source position may have canonical schema:

.. code-block:: text

   REF[TSL[REF[TS[int]], Size[2]]]

and the current alternative may expose:

.. code-block:: text

   TSL[TS[int], Size[2]]

The current ``RefLink`` reads the outer reference and binds to the
referenced output. If that referenced output is canonically:

.. code-block:: text

   TSL[REF[TS[int]], Size[2]]

then the target endpoint asks it for binding data for:

.. code-block:: text

   TSL[TS[int], Size[2]]

The referenced output owns the alternatives needed to dereference the
inner ``REF[TS[int]]`` elements. The outer ``RefLink`` only owns the
outer dereference boundary.

Notification flow
~~~~~~~~~~~~~~~~~

There are two modification paths:

REF source tick
    The source subscriber fires. The link reads the source
    ``TimeSeriesReference``, rebinds the target endpoint, marks the
    exposed alternative modified for the current evaluation time, and
    notifies downstream consumers of the dereferenced output.

Target tick
    The currently bound target output, or one of its structural child
    targets, ticks. The target endpoint propagates that modification
    through the ``RefLink`` so consumers of the dereferenced alternative
    observe the tick as if it came from a normal output of the exposed
    schema.

The source subscriber should not manage active subscriptions directly.
It only initiates rebind. Active subscription management belongs to the
target endpoint because the target endpoint is the component that knows
which output shape is currently exposed.

Active attachments
~~~~~~~~~~~~~~~~~~

To support multiple consumers downstream of one ``RefLink``, the state
holds per-input attachments keyed by the input notification identity
generated by the consuming ``TargetLink``. Each attachment carries the
consumer's forwarding ``SchedulingNotifier`` and its active-subtree
trie. Several inputs can therefore coexist below one dereference
boundary, each contributing a different active path set. When the
reference rebinds, the target endpoint replays every attachment's active
trie against the new target structure and unsubscribes stale active
paths from the old target structure.

Active state is therefore split deliberately:

- the ``RefLinkAttachment`` owns what a particular input asked to make
  active;
- the target endpoint owns the current subscriptions installed against
  the current target output structure.

When an input makes a descendant active below a ``RefLink``, the path is
recorded in that input's attachment trie and installed on the current
target endpoint if one is bound. When the same input makes the path
passive, the path is removed from that attachment. The target endpoint
can unsubscribe the corresponding target path when no remaining
attachment needs it.

Sampled semantics: when the reference retargets at tick *t*, the
runtime keeps a ``previous_target_value`` snapshot so a consumer that
reads ``delta_value`` at *t* sees the rebinding event coherently. (The
``retain_transition_value`` flag turns this off for internal
alternative-replay paths that resync structurally.)

RefLink is **only** used inside output alternatives, never on the
plain ``TSInput`` binding path. A REF input goes through TargetLink
binding to a REF output; the dereference happens on the output side
when an alternative needs to expose the referenced TS value.

Implementation invariants
~~~~~~~~~~~~~~~~~~~~~~~~~

The following invariants are useful when implementing or debugging
``RefLink``:

- the source subscriber observes exactly one ``REF`` source position;
- the target endpoint's exposed schema is fixed by the alternative
  request and never changes during rebind;
- a single-output reference always goes back through output binding
  negotiation, so canonical and alternative target outputs are handled
  uniformly;
- a non-peered reference recreates target structure under the current
  endpoint instead of flattening it to one output handle;
- downstream active tries are keyed by the consuming input notification
  identity, not by output path alone; this attachment layer is planned
  separately from the initial source-subscriber and target-endpoint
  implementation;
- rebind unsubscribes stale target subscriptions before installing or
  replaying subscriptions on the new target structure;
- views over a ``RefLink`` are transient cursors over binding data and
  must not be stored as link state.

ForwardingLink
--------------

ForwardingLink redirects one output position to another output
position. The state type is the output-only ``OutputLinkState``. It
carries:

- a stable handle to the forwarded-to output;
- a ``TargetNotifiable`` subscribed to that output so ticks propagate
  to the redirected position's subscribers;
- the same kind of previous-target handling as TargetLink so
  switch ticks read coherently.

Unlike TargetLink, ForwardingLink does **not** participate in input
scheduling. There is no scheduling notifier and no active input
subtree to manage — all that's needed is for an output's subscribers
to receive ticks from the forwarded-to output as if they had been
notified directly.

The canonical use case is the nested-graph boundary plan: the
``alias_child_output`` binding mode exposes a child node's output
directly as one of the parent node's outputs, by installing a
ForwardingLink at the parent output that points at the child's. No
value is copied; subscribers of the parent see the child's ticks.
Other nested-graph modes (``bind_bundle_member_output``,
``alias_parent_input``) compose ForwardingLink with structural
navigation through bundles or input fields.

Why three link states, not one
------------------------------

The three link types could in principle be unified into a single
"redirect" state with conditional logic. The runtime keeps them
separate because:

- TargetLink's two-notifier (target-modified + scheduling) split is
  load-bearing for the input scheduling contract; a uniform link
  doesn't need the scheduling-notifier-per-link identity machinery
  for output-only paths.
- RefLink owns source subscription, reference-driven target binding,
  structural non-peered references, and per-input active attachments;
  that machinery is wasted if hardwired into every link.
- ForwardingLink is the simplest of the three (no input scheduling,
  no source tracking) and benefits from a smaller state footprint.

The three types share the same lifetime invariant: borrowed target
pointers are only valid while the owning framework keeps the target
storage alive or explicitly unbinds the link first. Each one's
notification surface is shaped to its job.

Reference tokens over alternative-backed targets
------------------------------------------------

``TSInputView::reference()`` converts an input position to a
``TimeSeriesReference``. Two rules govern positions whose resolved target
lands on a from-REF alternative (added with the race-semantics work,
2026-07-05):

- **Bound alternative → reference TO the alternative position.** The
  consumer keeps following the alternative's retargets (liveness — the
  mesh dependency plumbing relies on this).
- **Unbound alternative → typed EMPTY reference.** The source reference
  emptied; the token must OBSERVE that invalidity (hgraph parity:
  ``race`` re-races when the winning candidate's reference items go
  empty). Collapsing further (chasing to the real output) is wrong in
  both directions: a snapshot of the current target breaks retarget
  liveness, and a live ref-to-ref never observes emptiness.

Two supporting rules from the same work:

- The ``structural_ref`` node declares an explicitly EMPTY
  ``valid_inputs`` set (not nullopt = "all fields"): it must re-evaluate
  when its source goes INVALID so consumers observe the emptied
  reference.
- ``race`` carries a second, always-active ``values`` input binding the
  candidates DEREFERENCED (hgraph's hidden ``_values``): reference
  inputs alone do not tick when a target merely becomes valid or
  invalid. ``race_ref_impl`` is deliberately NOT registered as a direct
  overload — its two-input shape would shadow the variadic graph
  overload.
- ``getitem_`` over a TSL holds its container input ACTIVE and dedupes
  same-reference re-publishes: element REBINDS must re-emit the item
  reference; plain value ticks must not.

Sampled rebinds through alternatives (2026-07-05 ruling)
--------------------------------------------------------

Howard's REF ruling: reference VALUES are opaque (storable/emittable;
never dereferenced via ``.output``); code needing the dereferenced value
accepts it as a (passive) input. Retargeting therefore must LOOK like a
tick to consumers — the sampled-runtime contract at the input boundary:

**Scalar and fixed-shape unbind is silent (hgraph parity).** When such a
reference goes EMPTY, the from-REF input unbinds - it reads not-valid from
that point - but consumers are NOT notified. A node observing the input
(e.g. ``valid``) keeps its last output until something else evaluates it.
Only a rebind to a live target records modified and samples.
(``test_tsd_validity_rebind`` pins this for scalar validity: routing away
from a branch must not tick ``valid`` False, and routing back to an unchanged
target re-evaluates but dedups.)

**Keyed structural unbind reconciles the published key set.** ``TSS`` and
``TSD`` are the exception: an EMPTY reference still makes the current value
invalid, but a previously visible collection produces one modified cycle with
removals for its published keys. A key added to the source in the same cycle
as the unbind was never published through the branch and therefore cannot
produce a removal. Likewise, a ``TSD`` slot whose child never became valid is
not a published dictionary entry and cannot produce a removal. A sampled
structural rebind similarly reports old-only keys as removed and new-only keys
as added; live ``TSD`` children in the new target are sampled as modified.

The link borrows the prior output's slot store for that reconciliation cycle.
It unsubscribes and stops the old graph on delete, while the slot protocol
keeps its storage alive until erase. No key/value snapshot or retired-entry
allocation is required. The borrowed handle is considered only while the
link's modification time matches the structural transition time.

- ``TSInputView::delta_value()``: when the position's LINK was modified
  after the target's own last tick (a from-REF retarget bound an
  already-valid output), the delta IS the current value — the target's
  delta storage belongs to an older cycle.
- ``InputDataCursor``: positions with their OWN link storage
  (``target_node == nullptr``) blend the link's tracking into
  ``modified()``/``last_modified_time()``. Descents within a root link
  tree do NOT blend the root's (shared) tracking — that would mark every
  sibling modified.
- A target position whose RESOLVED data is INPUT-SHAPED (a from-REF
  alternative holding per-child target links) projects children like an
  input (``child_from_resolved_input``): each child rides its own link
  position, so per-child rebind tracking reaches modified state and the
  sampled delta rule. The data-level element ops intentionally keep
  resolving THROUGH to the target (value-plan copies need the raw target
  memory).

Deferral: embedded is_empty / contains outputs (ruling 2026-07-06)
------------------------------------------------------------------

Python hgraph's TSS/TSD outputs embed auxiliary ``TS[bool]`` outputs
(``_is_empty_ref_output`` + ref-counted per-item contains outputs) that
update incrementally inside the set mutation; ``is_empty``/``contains_``
hand consumers REFs to them. **Not ported.** Here they are ordinary
operator nodes (``container_impl.h``): the check is an O(1) slot-store
probe and ``set_if_changed`` gives identical downstream tick semantics
(flips only). The embedded design's saving — one node evaluation per
container tick per check — mattered at Python node costs, not at C++
node costs; porting it would add aux-output lifecycle machinery and a
second way to derive a ``TS[bool]`` from a set (the parallel-abstraction
smell). If profiling ever shows a hot set with many contains consumers,
the alternative store is the natural home for an embedded variant.
