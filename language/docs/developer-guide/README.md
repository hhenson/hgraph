# Developer Guide

This guide explains how to implement the source contract in the
[User Guide](../user-guide/README.md). The compiler is an authoring frontend
for hgraph, not a second runtime.

> **Implementation status:** `src/syntax/` implements the lexer, the
> temporal literal parser, the arena AST, and the parser of
> [Syntax and semantics](syntax-and-semantics.md), including `requires`,
> nominal and generic `struct`, abstract-only inheritance, `null`, and
> structured `delta` forms. `src/semantics/` binds constraint names, resolves
> struct families and effective fields, validates hierarchy and generic
> argument roles, and classifies functions.
> `src/ir/` now lowers every resolved guide example into a source-ranged HIR
> arena with stable declaration and symbol identities, then completes canonical
> types, substitutions, constraints, calls, phases, effects, and capabilities.
> It validates constrained generic structs in every type position and leaves a
> failed module explicitly `Resolved` rather than claiming `Typed` completion.
> `src/hgraph_ir/` lowers typed HIR into independently owned canonical types,
> compile-time expressions, nominal operator contracts, and callable
> interfaces. That checkpoint is explicitly interface-only; executable graph
> and runtime-node plans and backend migration are the next stacked changes.
> `src/wiring/` executes the composition subset for `test`, `run`, and the
> REPL, including scalar and atomic struct values, type-only generic
> specializations, and field-wise temporal struct composition. `src/codegen/`
> emits every checked-in example as public hgraph C++, including nominal and
> generic structs, generic operators and windows, sparse deltas, runtime
> collection traversal, activation, aggregate scalar recordable state, output,
> logger injection, and lifecycle hooks over state and `const` configuration.
> The driver compiles and caches/loads that subset for file-based `test`, `run`, and REPL sessions on
> Unix. REPL replacement stages the new image, swaps removable provider handles
> at a quiescent boundary, and restores the old revision if activation fails.
> Imported operator-contract conformance, arbitrary residual `const` predicates, `const`
> generic native metadata, multiple-parent linearization, explicit optional-field
> clearing, multi-registry module transactions, and the remaining runtime and
> generated-C++ type support remain to be implemented.

## Guide map

1. [Syntax and semantics](syntax-and-semantics.md) defines `fn` syntax,
   `export fn`, bodyless nominal `operator` contracts, generics, module aliases,
   `requires` constraints and type substitution, `const` parameters, lexical
   bindings, canonical and rolling types, nominal and generic structs,
   abstract data families, final concrete values, inherited defaults,
   generic construction and constraints, sparse deltas,
   recursive temporalization, metadata, and collection iteration, plus
   provisional state, injectable, lifecycle, activation, and output semantics,
   and the `test`, `eval`, and run model.
2. [Compiler and C++ lowering](compiler-and-lowering.md) defines the frontend
   pipeline, function classification, public SDK lowering, the direct-wiring
   backend, generated module lifecycle, source mapping, and build manifests.
3. [Testing and compatibility](testing-and-compatibility.md) defines syntax,
   type-shape, classification, harness, generated-code, installed-SDK, and
   backend-parity acceptance.

The design records provide project boundaries and rationale:

- [Architecture](../design/architecture.md)
- [Compiler architecture](../design/compiler-architecture.md)
- [Language model](../design/language-model.md)
- [Modules and native extensions](../design/modules.md)
- [Native interface](../design/native-interface.md)
- [Documentation architecture](../design/documentation.md)
- [Architecture decisions](../design/decisions/README.md)
- [Roadmap](../design/roadmap.md)

## Sources of truth

When documents disagree, resolve them in this order:

1. accepted language RFCs, once the RFC process exists;
2. the User Guide for observable behavior;
3. this guide for compiler and tooling behavior;
4. design records for architectural constraints;
5. examples, which must be updated with their owning guide.

Generated C++ is an implementation artifact, not a source compatibility
surface.

## Current invariants

- `fn` is the only implementation declaration; `operator` declares a bodyless
  nominal callable contract, and `impl fn` is the only way to implement one.
- Every `operator` and `impl fn` candidate is public by definition;
  an ordinary exact function is module-internal unless declared `export fn`.
- Imports expose names but do not activate providers; the locked package target
  defines the complete candidate universe without declaration re-exports.
- Ordinary parameters and results use canonical recursively temporal types.
- The temporal scalars are `date`, `time`, `datetime`, `duration`,
  `civil_datetime`, `timezone`, `zoned_datetime`, and `zoned_time`, mapping
  to hgraph's RFC 0002 types (`zoned_time` is the one hgraph-side addition);
  `@` literals and unit-suffixed durations are single validated tokens, and a
  literal never chooses a fold or gap policy silently.
- `atomic<T>` stops recursive temporalization at `T`.
- A nominal `struct` has a canonical Bundle value, recursively temporalized
  TSB shape, named-only construction, required/default/optional fields, and an
  explicit `atomic<S>` snapshot boundary. Only abstract structs are bases;
  concrete structs are final, while descendants may replace defaults but not
  field types or optionality.
- Generic structs form invariant nominal families over canonical value types
  and wiring-time constants. Types are fully applied; constructors may infer a
  complete substitution, and `requires` validates each specialization.
- `delta<S>(...)` is a contextual sparse update: omitted fields mean no change,
  defaults do not apply, and `null` explicitly clears only optional fields.
- `rolling<T, max_size[, min_size]>` maps to a TSW shape whose sizes are
  wiring-time tick counts or durations, one kind per window, and part of its
  type identity.
- `list<T>` is an unbounded temporal list and `list<T, n>` a fixed one;
  `unbounded` is the size sentinel a `const` size generic may bind.
- A structural `tuple<...>` is hgraph's un-named bundle with positional
  fields; it is never a list.
- `const` parameters are wiring-time values and use their canonical value type
  directly.
- Plain generic parameters bind canonical source types; `requires` constraints
  drive substitution and admission before wiring-time candidate ranking.
- `let` locals are immutable; `var` locals are mutable but lexical and
  evaluation-local rather than recordable state.
- Source does not spell hgraph `TS`, `TSB`, `TSL`, `TSS`, `TSD`, or `TSW`
  wrappers.
- Source does not expose endpoint `.value`, `.valid`, or `.modified` members.
- Runtime collection traversal uses `keys`, `values`, and `items` with optional
  built-in, named, or inline predicates; its borrowed iterators cannot escape an
  evaluation.
- Selective imports establish the unqualified operator names an `impl fn` may
  bind to; module aliases provide qualified names such as `mm::my_op` without
  binding implementations.
- Name resolution selects one nominal operator identity before hgraph performs
  candidate normalization, ranking, and diagnostics.
- A body without runtime-only constructs is classified as composition;
  `state`, `inject`, `start`, `when`, `stop`, or runtime collection iteration
  classifies the complete `fn` as a runtime node.
- Runtime `when` predicates are decomposed into activation, validity admission,
  and residual per-evaluation logic where possible.
- State declarations aggregate into one recordable state value; grouped
  inject declarations map approved capabilities to native selectors.
- `return value` is terminating output, while `inject out` enables persistent
  output inspection and incremental mutation.
- Classification remains a semantic stage over common syntax and HIR; the
  parser must not directly emit graph- or node-specific declarations.
- Scripted, REPL, and ahead-of-time workflows consume the same classifier,
  checked IR, and C++ backend.
- Compiled modules use generated, handle-owned initialization, replayable
  installation, registration removal, and reverse-order deinitialization.
- Live graphs and plans retain provider leases; registration removal precedes
  safe native-library unloading.
- Hgraph core has no dependency on the language project.
