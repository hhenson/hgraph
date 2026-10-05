HGL complete atomic family publications
=======================================

An abstract struct family may declare the type of a complete atomic payload.
Ordinary ancestor bindings retain the exact concrete member, including fields
absent from the parent. Generic ancestry is checked after substituting each
specialization; unrelated values and mismatched specializations are rejected.
Abstract structs remain nonconstructible.

Prepared ordinary storage uses the existing closed-family realization. Module
registration registers every emitted concrete member, including members used
only by pass-through signatures. Registration does not capture family plans.
Cold ordinary construction captures after providers have been installed; node
preparation captures family-dependent plans in the node's existing prepared
state. Graph construction freezes the alternatives, and publication does not
discover new members or consult registries. Complete preflight checks membership
and validates required fields against the concrete member's source contract.

An ordinary subfamily-to-ancestor conversion retains the source's live concrete
member into the ancestor plan. It never copies a subfamily union's storage as if
it were a concrete member, and retains nested children independently.

Each publication replaces the whole member. Equal-layout descendants retain
distinct tags, optional fields retain presence, and mutable children are owned
independently by captures. Generic pass-through, ordinary containers, replay
and recording preserve both the declared family and concrete values.

Structural family roots, recursive family publications and unresolved
multiple-parent field ordering remain outside this admission. The normative
contract is ``abstract-atomic-publications.md`` in the pinned specification.
