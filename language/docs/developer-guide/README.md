# Developer Guide

This guide explains how to implement the source contract in the
[User Guide](../user-guide/README.md). The compiler is an authoring frontend
for hgraph, not a second runtime.

> **Implementation status (audited against main, 2026-09-19):** the frontend
> parses and resolves source, completes typed HIR, and lowers it to hgraph IR.
> Both execution backends consume that IR. Shared admission and activation
> planning lives in `src/hgraph_ir/plan.cpp`; runtime semantics belong to hgraph.
> Composition, scalar value functions, supported runtime functions, scalar
> state or cache, lifecycle capabilities, native helpers, and supported
> imported operator implementations are executable. Scripted native loading
> remains Unix-only; ahead-of-time packages also support Windows.
>
> The [status matrix](../design/roadmap.md#feature-status-matrix-2026-09-07)
> records the boundaries. In particular, enum/switch syntax, imported contract
> constraints/properties, generic constructor inference,
> optional-field clearing, and wiring-time reference dereference are not
> implemented. Later target mappings in this guide do not override those limits.

## Guide map

1. [Syntax and semantics](syntax-and-semantics.md) defines `fn` syntax,
   `export fn`, bodyless nominal `operator` contracts, generics, module aliases,
   `requires` constraints and type substitution, `const` parameters, lexical
   bindings, canonical and rolling types, nominal and generic structs,
   abstract data families, final concrete values, inherited defaults,
   generic construction and constraints, sparse deltas,
   recursive temporalization, metadata, and collection iteration, plus
   implemented state, cache, injectable, lifecycle, activation, and output semantics,
   and the `test`, `eval`, and run model.
2. [Compiler and C++ lowering](compiler-and-lowering.md) defines the frontend
   pipeline, function classification, public SDK lowering, the direct-wiring
   backend, generated module lifecycle, source mapping, and build manifests.
3. [Testing and compatibility](testing-and-compatibility.md) defines syntax,
   type-shape, classification, harness, generated-code, installed-SDK, and
   backend-parity acceptance.
4. [Control-flow scenarios and C++ mappings](control-flow-cpp-mappings.md)
   pairs HGL source with the expected node and graph lowerings for the agreed
   switch and conditional-result scenarios. These are reference mappings,
   not claims of implemented HGL switch support.
5. [Enum source and C++ mappings](enum-cpp-mappings.md) pairs numbered HGL
   declarations with illustrative C++ values and member-name strings, and
   records explicit and automatic duplicate-number errors. It also maps
   `str(value)` in constants, node evaluation, and temporal graph composition,
   and shows declaration-order enumeration with non-monotonic member numbers.
   Checked `Mode(...)` conversion covers assigned integers, exact member-name
   strings, and errors for unknown values.
   These remain design fixtures, not implemented HGL enum/conversion support.
6. [Operator source and C++ mappings](operator-cpp-mappings.md) pairs executable
   graph and node arithmetic with native wiring and scalar kernels, and explains
   how domain properties survive lowering without becoming optimizer proofs.

7. [Native modules and packages](native-modules-and-packages.md) covers C++
   helpers, descriptors, generated packages, module ownership, and the scripted
   loader. These are extension/toolchain details, separate from HGL source usage.

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

- The implemented temporal model uses `fn`; `operator` declares a bodyless
  nominal callable contract, implemented by `impl fn`. Local fixed-arity
  `const fn` and [default lifting](../user-guide/value-functions.md) are implemented; see
  [ADR 0008](../design/decisions/0008-temporal-contracts-and-target-mappings.md)
  for phase eligibility, cache/state, and target-mapping follow-up work.
- Every `operator` and non-generic `impl fn` candidate is public by definition;
  generic implementations contribute only their explicit `instantiate`
  materializations, whose `_` arguments may retain resolver slots, and an
  ordinary exact function is module-internal unless declared `export fn`.
- Imports expose names but do not activate providers; explicit package dependencies
  select providers without declaration re-exports. Automatic transitive lock-file
  discovery remains planned.
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
  and wiring-time constants. Types and constructors require explicit
  generic arguments today; `requires` validates each specialization.
- `delta<S>(...)` is a contextual sparse update: omitted fields mean no change,
  defaults do not apply. Explicit clearing with `null` is reserved but rejected.
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
- Runtime collection traversal uses `keys`, `values`, `elements`, and `items`
  with optional built-in, named, or inline predicates; its borrowed iterators
  cannot escape an evaluation. `values` projects from keyed or named
  collections, while `elements` traverses lists and sets.
- Selective imports establish the unqualified operator names an `impl fn` may
  bind to; module aliases provide qualified names such as `mm::my_op` without
  binding implementations.
- Name resolution selects one nominal operator identity before hgraph performs
  candidate normalization, ranking, and diagnostics.
- A body without runtime-only constructs is classified as composition;
  `state`, `cache`, `inject`, `start`, `when`, or `stop` classifies the complete `fn` as
  a runtime node. Collection iteration follows the containing phase and does
  not classify the function by itself.
- Runtime `when` predicates are decomposed into activation, validity admission,
  and residual per-evaluation logic where possible.
- A handler with no modification selector defaults to any temporal input; one
  with no validity selector defaults to all temporal inputs being top-level
  valid. Bare `when { ... }` supplies both defaults.
- State declarations aggregate into one recordable state value; cache declarations
  instead aggregate into rebuildable state. Combining them is rejected. Grouped
  inject declarations map approved capabilities to native selectors.
- `return value` is terminating output, while `inject out` enables persistent
  output inspection and incremental mutation.
- Classification remains a semantic stage over common syntax and HIR; the
  parser must not directly emit graph- or node-specific declarations.
- Scripted, REPL, and ahead-of-time workflows consume the same classifier,
  checked IR, and C++ backend.
- Scripted modules use generated, handle-owned initialization, replayable
  installation, registration removal, and reverse-order deinitialization.
- Live graphs and plans retain provider leases; registration removal precedes
  safe native-library unloading. Ahead-of-time packages still use explicit
  generated registration/removal functions; their lifecycle ABI is follow-up work.
- Hgraph core has no dependency on the language project.
