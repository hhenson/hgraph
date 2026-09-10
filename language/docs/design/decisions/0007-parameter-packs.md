# ADR 0007: explicit parameter-pack shapes

Status: accepted and implemented for signatures, calls, composition traversal,
module descriptors, and generated C++ operator contracts

## Context

Hgraph operators need three materially different variadic shapes. Treating all
of them as one anonymous bundle would either erase useful type relationships or
leak the generated `_0`, `_1`, ... field names used by some native
representations into HGL source.

## Decision

HGL spells the three shapes explicitly:

```hgl
// Repeated values with one shared type. Positional only; native shape is TSL.
operator homogeneous<T>(values: ...T) -> T

// Positional values whose types may differ. HGL exposes a structural tuple.
operator positional<...Ts>(values: ...Ts) -> i64

// Named values whose names and types may differ. HGL exposes a bundle.
operator keyword<...Fields>(values: ...{Fields}) -> i64
```

The declaration, not the arguments at one call site, determines the shape.
Supplying equal types to a heterogeneous pack does not turn it into a
homogeneous pack.

A homogeneous positional pack unifies every captured value with the same `T`.
A heterogeneous pack checks each member independently and does not require a
common type. A type-pack name such as `Ts` or `Fields` is not a singular type:
it may only appear through its corresponding pack parameter and pack
reflection.

Fixed parameters are bound first. Remaining positional arguments enter the
positional pack; unmatched named arguments enter the named pack with their
source names intact. A call cannot repeat a fixed name, and a positional
argument cannot follow a named argument. Packs have no default and may be
empty. An empty homogeneous pack must obtain `T` from another position or the
call is not resolvable.

Candidate ordering remains owned by hgraph's operator resolver. The intended
specificity is fixed arity before a homogeneous pack before a heterogeneous
positional pack. HGL emits `In`/`VarIn`/`VarKwIn` selectors and does not
reimplement native overload ranking.

## Source views

The positional heterogeneous view behaves as `tuple<...>` even if generated
C++ uses a private bundle representation:

```hgl
for value in elements(values) { ... }
for index, value in items(values) { ... } // zero-based i64 index
```

The named heterogeneous view preserves names:

```hgl
for name in keys(values) { ... }
for value in values(values) { ... }
for name, value in items(values) { ... }
```

Private `_0`, `_1`, ... names are never HGL keys and must not appear in source
diagnostics, descriptors, or documentation. Composition traversal expands at
wiring time and passes erased ports to the ordinary hgraph resolver, so each
heterogeneous member retains its concrete schema.

## Staged boundary

This change implements the representation needed by operator contracts and
composition functions. Runtime-node pack parameters still fail closed because
they require an agreed native aggregate input-view ABI. Type-pack reflection
inside `requires` (including minimum arity and per-member constraints) also
remains open; no comparison or pack-fold syntax is invented here. Those two
items must be agreed before runtime or constrained heterogeneous
implementations are accepted.

