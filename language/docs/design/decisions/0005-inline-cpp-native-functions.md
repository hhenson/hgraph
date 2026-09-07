# ADR 0005: Module-local exact native functions may contain C++

Status: accepted and implemented for evaluation-time value/view functions

## Context

HGL must express most hgraph graphs and nodes without becoming a general-purpose
language. Some node calculations nevertheless need a small amount of direct
C++ over current scalar values or live collection views. Requiring every such
helper to exist first in a separately built C++ package creates duplicate HGL
and C++ declarations and slows standard-library migration.

The native escape must remain distinguishable from graph wiring, hgraph node
construction, adaptor ownership, and module lifecycle. Generated code must
also remain readable enough to review as ordinary hgraph C++.

## Decision

Admit one top-level source form:

```hgl
native fn len<T, const size: i64>(value: list<T, size>) -> i64 {
    cpp(const hgraph::TSLInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}
```

The HGL signature owns the function identity, overload pattern, constraints,
parameter access, and result type. The `cpp(...)` projection owns only the C++
parameter declarations. The compiler supplies a C++ result type and name,
emits a `noexcept` plain function in the generated module's `native` namespace,
and calls it directly. Source native functions are automatically public and
appear as exact native declarations in the generated module descriptor.

The initial form is evaluation-only and stateless. Collection signal arguments
receive live hgraph input views; other arguments receive values. HGL generic
parameters select an overload but do not generate a C++ template. Requirements
are rejected until the version-one descriptor catalog can reconstruct and
enforce them for downstream imports. Native parameter defaults are not
supported.

The HGL lexer recognizes a balanced C++ parameter list and compound statement,
including nested delimiters, comments, quoted literals, escapes, and raw string
literals. It does not parse or reinterpret the C++ body. The C++ compiler is the
authority for that projection, and normal generated-code `clang-format` runs
before the artifact is written or compiled.

No source syntax is added for includes, external link dependencies, effects,
throwing functions, state types, lifecycle phases, or ownership. Those remain
descriptor/package concerns until separate decisions define them.

## Consequences

- Small native algorithms can live beside the HGL nodes that use them.
- Calls compile to direct, human-readable C++ without an operator subclass or
  per-tick type dispatch.
- Downstream HGL modules import the generated descriptor exactly as they import
  external native declarations.
- `hgl check` validates the HGL contract and balanced C++ boundaries;
  `emit-cpp` additionally validates the generated descriptor against the
  version-one native ABI, and only native compilation can validate the C++
  itself.
- Native adaptors, resources, headers, and independently distributed libraries
  continue to use the descriptor boundary from ADR 0003.
