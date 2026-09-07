# hgraph language documentation

The documentation is split by audience:

- The [User Guide](user-guide/README.md) shows how language functions and
  canonical temporal types look.
- The [Developer Guide](developer-guide/README.md) explains how the compiler
  parses, checks, lowers, builds, and tests those programs.
- The design records capture architectural decisions, project boundaries, and
  the delivery roadmap.

The language is still a design preview. The current `hgl` checks every
example (`hgl check`), runs composition functions directly, compiles and
loads the scalar runtime-node subset for file-based `hgl test` and `hgl run`
and the REPL on Unix, and emits the same C++ through `hgl emit-cpp` for
`hgl_add_module()`. The status of every language surface, with its
fail-closed boundary or named blocker, is kept in one place: the
[roadmap status matrix](design/roadmap.md#feature-status-matrix-2026-09-07).
Provisional syntax is labelled so that examples do not imply an implemented
compatibility promise.

## Design records

1. [Architecture](design/architecture.md) — ownership, compiler pipeline,
   the two backends, and execution modes.
2. [Language model](design/language-model.md) — functions, nominal operators,
   generics, generic constraints and substitution, exports, canonical and
   rolling temporal types, nominal and generic structs, abstract data families,
   generic construction, inherited defaults, optional fields, sparse deltas,
   runtime state, lifecycle, activation, output, tests and running, and syntax
   decisions.
3. [Compiler architecture](design/compiler-architecture.md) — source fidelity,
   pass contracts, typed HIR, hgraph IR, backend boundaries, and the parser
   migration.
4. [Modules and native extensions](design/modules.md) — how C++ packages become
   importable, contribute overloads, and participate in generated module
   initialization and deinitialization without exposing a general FFI.
5. [Native interface](design/native-interface.md) — top-level C++ value/view
   helpers, descriptor-backed external kernels, opaque state, and
   phase/effect/ownership metadata.
6. [Documentation architecture](design/documentation.md) — audience boundaries,
   feature status, executable examples, and code documentation.
7. [Architecture decisions](design/decisions/README.md) — numbered decisions
   that constrain several compiler passes or artifacts.
8. [Roadmap](design/roadmap.md) — vertical slices, the compiler architecture
   stack, core-library migration, and acceptance gates.
9. [Distribution and deployment](design/distribution.md) — release train,
   package channels (Homebrew first), the relocatable native context, and
   what a host needs to run an HGL program.
10. [Type extensions](design/type-extensions.md) — agreed atomic treatment of
   imported types, `ref<T>`, reference-transparent type compatibility, opaque
   node access, wiring-time dereferencing, input-only `signal` observation,
   enum declaration/member syntax, explicit and automatic numbering,
   member-name stringification through `str(value)`, checked construction
   through `Mode(...)`, and duplicate-number rejection.
11. [Conditional control flow](design/control-flow.md) — wiring-time selection,
   switch-style temporal conditions in graph functions, and node conditionals.
12. [Explicit switch dispatch](design/switch.md) — node-style C++ dispatch,
   graph selector checks and branch signatures, default handling, and no-match
   failure, with agreed source syntax and constant case values.
13. [Iteration](design/iteration.md) — phase-dependent `for`, wiring-time values
   and fixed child connections, independent dynamic graph loops, runtime
   traversal, and deferred map/reduce accumulation.
14. [Backend-neutral runtime specification](design/runtime-specification.md) —
   an exploratory `.hgspec` contract for generating runtime type, ownership,
   lifecycle, operator, and conformance surfaces across implementation
   languages without turning HGL into a systems language.

An accepted change should update the relevant guide and its owning design
record together. The user guide is the source of truth for observable language
behavior; the developer guide is the source of truth for implementation
constraints.

## Standard-library design corpus

The [standard-library folder](../stdlib/README.md) collects agreed HGL examples
to drive core-library coverage and expose missing language features. Its
[migration inventory](../stdlib/inventory.md) and deliberately provisional
[HGL source root](../stdlib/hgl/README.md) now provide a second, non-executable
track for testing the whole native library against the language design. The
agreed corpus starts with conditional-result examples; the fixed-list and
independent dynamic collection cases have graduated into the compiler's
runnable example corpus. A single escaping conditional result and a bundle of
several escaping results have also graduated, as have a used expression result
combined with escaping assignments and reference-preserving forwarding of
initialized results.
Value-producing temporal conditionals without `else` have also graduated with
a typed never-ticking false path, and so have early-return continuations
(`examples/conditional-early-return.hgl`). The `switch`, enum, `str(value)`,
and `elements` fixtures remain in the design corpus. A complete component
catalogue remains to be developed. Files left in the design corpus or source
prototype are not a claim of implemented support.
