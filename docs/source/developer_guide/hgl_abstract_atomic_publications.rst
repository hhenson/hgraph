HGL complete atomic family publications
=======================================

An abstract struct family may declare the type of a complete atomic payload.
Ordinary ancestor bindings retain the exact concrete member, including fields
absent from the parent. Generic ancestry is checked after substituting each
specialization; unrelated values and mismatched specializations are rejected.
Abstract structs remain nonconstructible.

Prepared ordinary storage uses the existing closed-family realization. All
known generated ordinary schemas are registered before their plans are
captured. Graph construction freezes the family alternatives; publication does
not discover new members. Complete preflight checks membership and validates
required fields against the concrete member's source contract.

Each publication replaces the whole member. Equal-layout descendants retain
distinct tags, optional fields retain presence, and mutable children are owned
independently by captures. Generic pass-through, ordinary containers, replay
and recording preserve both the declared family and concrete values.

Structural family roots, recursive family publications and unresolved
multiple-parent field ordering remain outside this admission. The normative
contract is ``abstract-atomic-publications.md`` in the pinned specification.
