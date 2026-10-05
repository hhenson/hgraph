HGL recursive atomic publications
=================================

Concrete recursive structs admitted by ADR 0012 can be complete atomic
publication payloads. Optional direct atomic edges hold finite owned values;
unset edges terminate the tree. Self recursion, mutual recursion and finite
generic specializations preserve their exact nominal schemas at every edge.

Schema preparation follows ownership boundaries without expanding recursive
fields inline. Eval preflight walks present values, checks required fields at
every depth and rejects cycles. Whole-value publication replaces all previous
descendants. Equality and retained captures include deep field values and
presence; mutable children remain independent of their source and siblings.
The same payloads can occur in ordinary containers or atomic children of
structural publications, and in replay and recording.

This admission does not permit recursive nominal roots as structural delta
shapes, container-based recursive declarations, required recursive edges,
unbounded generic specialization or abstract recursive families. Their existing
checks remain in force. The normative contract is
``recursive-atomic-publications.md`` in the pinned specification.
