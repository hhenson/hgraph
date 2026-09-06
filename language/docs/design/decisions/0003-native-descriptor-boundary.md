# ADR 0003: Native code is exposed by descriptors, not inline source

Status: accepted; descriptor authoring format and lifecycle ABI pending

## Context

Runtime-node implementations need efficient access to selected C and C++
libraries, but arbitrary native source would blur wiring and evaluation phases,
escape the type and ownership system, and turn HGL into a general-purpose
language.

Cppfront permits C++ and Cpp2 only
[side by side rather than nested](https://hsutter.github.io/cppfront/cppfront/mixed/),
because even two syntaxes for the same C++ object model become semantically
ambiguous when nested. HGL has the additional distinction between wiring and
tick-time execution, so pass-through source is a still poorer fit.

Swift's [C++ interoperability](https://www.swift.org/documentation/cxx-interop/)
shows the other end of the spectrum: an embedded Clang importer, module maps,
direct calls, and explicit annotations where ownership and dependent lifetimes
cannot be inferred. HGL adopts the explicit module and ownership lesson without
importing the general C++ language.

## Decision

Expose reviewed native declarations through versioned module descriptors and
package-provided wrappers. Ordinary HGL contains no inline C++, preprocessor,
raw pointer, or header-import escape. The first slice supports canonical scalar
evaluation functions and owned opaque node state under the restrictions in
[Native interface](../native-interface.md).

## Consequences

- Compiled calls can remain direct C++ calls without a general FFI.
- `hgl check` can validate imports without loading executable code.
- Phase, effect, ownership, exceptions, and build dependencies are explicit.
- A later Clang-based generator may produce descriptors but cannot widen the
  language implicitly.

## Alternatives

- Pass through side-by-side or nested C++: rejected for ordinary HGL.
- Import arbitrary headers with an embedded C++ compiler: deferred as excessive
  machinery for a deliberately narrow DSL.
- Require every scalar helper to be an hgraph operator: rejected because
  private runtime-node implementation state needs exact native value calls, not
  additional graph topology.
