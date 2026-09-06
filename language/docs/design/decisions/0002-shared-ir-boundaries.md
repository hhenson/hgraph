# ADR 0002: Typed HIR and hgraph IR are mandatory backend boundaries

Status: accepted

## Context

The prototype annotates the syntax AST in `ResolvedModule`. Direct wiring and
C++ generation then walk that representation independently, which permits type,
phase, lowering, and diagnostic behavior to diverge.

## Decision

Introduce typed HIR as the complete language-semantic representation and hgraph
semantic IR as the only backend input. Both direct wiring and C++ generation
consume hgraph IR and may not include syntax AST headers after migration.

## Consequences

- `hgl check` can validate one representation shared by every execution mode.
- Function classification, generic substitution, and call resolution happen
  once.
- Backend parity compares code generation rather than duplicated semantic
  implementations.
- Temporary resolved-AST adapters are removed at the end of the migration
  stack.

## Alternatives

- Continue annotating AST nodes: rejected because backend behavior is already
  implemented twice.
- Emit C++ directly from HIR: rejected because direct wiring and future
  backends need an explicit representation of hgraph semantics.
