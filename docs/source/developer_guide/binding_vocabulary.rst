Binding Vocabulary
==================

Binding names combine terms from independent axes. Read a phrase such as
``sampled keyed rebind`` as:

``sampled``
   How state becomes observable at the binding boundary.

``keyed``
   The shape whose state is being reconciled.

``rebind``
   The lifecycle operation being performed.

The adjectives do not imply one another. In particular, ``structural`` does
not mean sampled, peered does not mean owned, and active does not mean valid.

Shape and topology
------------------

Structural
   State with child or membership topology, rather than only one atomic value.
   The word has three qualified uses in this codebase:

   - a **structural source/endpoint** is assembled from child sources instead
     of naming one producer for the whole position;
   - a **structural event** changes topology or membership, such as adding a
     ``TSD`` key, rather than merely ticking an existing child value; and
   - a **keyed structural transition** reconciles ``TSS``/``TSD`` membership
     across a rebind or unbind.

   A collection can still be peered as one whole endpoint. Therefore
   "structural type" and "non-peered endpoint" are not synonyms.

Fixed structural
   A ``TSB`` or fixed-size ``TSL`` whose child positions are known when the
   graph is wired.

Keyed structural
   A ``TSS`` or ``TSD`` whose live membership is known only at runtime and is
   managed by the slot-store protocol.

Endpoint role
-------------

Peered
   The complete annotated position names one producing output and binds through
   one target link. Descendants below that point are reached through the same
   producer identity.

Non-peered
   The position is a local structural prefix. Its children independently name
   peered, non-peered, or owned endpoints. Wiring calls this a **structural
   source** because the source itself is the child topology, not a producer.

Owned
   The endpoint has local ``TSData`` payload storage instead of delegating to a
   producing output.

Binding operations
------------------

Bind
   Attach an endpoint to a target, subscribe to its notifications, and make its
   current state readable. A plain bind does not by itself promise a sampled
   tick.

Unbind
   Detach the current target and its subscriptions. The endpoint becomes
   unbound and therefore invalid. Scalar and fixed-shape REF unbinds are
   intentionally silent.

Rebind or retarget
   Replace one target with another while preserving the consuming endpoint's
   identity. Conceptually this is detach plus attach, with transition semantics
   determined by the shape and delivery policy.

Sampled
   Publish the target's current state at the bind time, even if the target did
   not tick in that engine cycle. This is not polling, periodic sampling, or a
   second copy of the source. Nested graphs use normal scheduling notification
   to evaluate against that current state.

   For keyed structures, a sampled rebind is a reconciliation: old-only keys
   are removed, new-only keys are added, and live ``TSD`` children on the new
   target are sampled as modified.

The current method names apply those rules directly:

``bind_sampled``
   Bind or retarget using sampled delivery semantics.

``unbind_structural``
   Unbind a keyed ``TSS``/``TSD`` while exposing one removal delta for entries
   previously published through the binding. A same-cycle addition that was
   never exposed, and a ``TSD`` child that never became valid, cannot produce a
   removal.

There is no separate delivery policy called a **structural bind**. Use that
phrase only for "bind a structural source/endpoint", and qualify it when the
intended meaning is narrower: ``non-peered structural binding``, ``key-set
structural event``, or ``sampled keyed rebind``.

Runtime state
-------------

Bound / unbound
   Whether a link currently has a target. A bound target may still be invalid.

Valid / invalid
   Whether the time-series position currently has a value. This is independent
   of whether the position is bound or active.

Current value / delta
   The persistent state readable now, versus the change published for one
   engine cycle. A sampled operation derives a delta from current state.

Modified / scheduled
   Modified records that state changed at an engine time. Scheduled means a
   node has been queued to evaluate. Notification usually connects them, but
   they are distinct states.

Published
   State that has crossed the endpoint boundary and may therefore require a
   later compensating removal. Merely allocating a slot is not publication.

Active / passive
   Whether changes on an input schedule its consuming node. Both active and
   passive inputs remain readable; passivity changes scheduling, not binding,
   validity, or ownership.

Link names
----------

``TargetLink``
   Input-to-output binding used by ordinary peered inputs.

``RefLink``
   Output-side adapter whose target is selected by a ``REF`` value.

``ForwardingLink``
   Output-to-output alias used to export a nested graph output without copying
   it.

See :doc:`data_structures/linking_strategies` for their storage, notification,
and lifetime contracts.
