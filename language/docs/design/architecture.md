# Architecture

Status: initial design

## Purpose

The hgraph language is a domain-specific authoring language for typed
functions over hgraph. It gives authors a compact, learnable surface while
preserving the C++ hgraph runtime as the source of truth.

The project is hosted beside hgraph while the design matures, but it is an
independent consumer of the public hgraph SDK. It must remain possible to move
the directory into a separate repository without changing hgraph core.

## Goals

- Express typed functions that lower to hgraph graphs or nodes and call
  registered operators.
- Express nominal generic operator contracts whose `fn` implementations reuse
  hgraph candidate matching and ranking.
- Expose ordinary exact functions explicitly with `export fn`, while treating
  operator contracts and their bound implementation candidates as public by
  definition.
- Make wiring-time and tick-time code visibly different and statically checked.
- Produce ordinary C++ that uses public hgraph authoring APIs.
- Offer source-first `check`, `run`, and REPL workflows for exploration.
- Produce reproducible ahead-of-time artifacts for deployment.
- Import capabilities from hgraph extensions without making them core
  dependencies of the compiler or generated programs that do not use them.
- Preserve the same graph, node, type, overload, lifecycle, and record/replay
  semantics as native C++ hgraph authoring.

## Non-goals

- A general-purpose systems language.
- A general C or C++ FFI available to language authors.
- Defining transports, push adaptors, callbacks, threads, or external resource
  ownership in language source.
- A second graph runtime, scheduler, type resolver, or operator dispatcher.
- Reimplementing hgraph behavior in an interpreter for convenience.
- Treating generated C++ as the stable user-facing language contract.

## Ownership and dependency direction

Dependencies point in one direction:

```text
language program
      |
      v
hgraph-language compiler ----> selected language module descriptors
      |                                      |
      v                                      v
generated C++ ----------------> hgraph and selected extension SDKs
      |
      v
hgraph runtime
```

Hgraph core never imports, links, or otherwise depends on the language
project. The compiler requires hgraph. Extension integrations are optional and
are selected by the application target's locked package dependencies. Source
imports control names, not which implementation providers are active.

## Semantic phases

Source uses one `fn` declaration rather than exposing graph and node keywords.
A function-classification stage maps explicit source syntax to hgraph's
backend phases:

| Backend kind | Phase | May do | Must not do |
| --- | --- | --- | --- |
| Composition | Wiring | Compose functions, pass ports, inspect `const` values, select fixed topology | Read current time-series values, keep runtime state, perform runtime side effects |
| Compute or sink | Evaluation | Read admitted runtime inputs, update declared state, produce the contracted tick or side effect | Add topology, resolve overloads, acquire arbitrary external resources |
| Imported adaptor or service | Native C++ | Own callbacks, threads, queues, protocols, and resources through hgraph lifecycle contracts | Expose unrestricted native execution to language source |

The current provisional rule classifies an ordinary body as composition. A
`state` or `inject` declaration, a `start`, `when`, or `stop` block, or runtime
collection iteration classifies the complete function as runtime evaluation.
Composition bodies flatten through hgraph wiring, while runtime bodies lower
to typed static C++ hooks whose instance data uses hgraph selectors and plans.

Function-level state declarations aggregate into one typed state value.
Function-level inject declarations request an approved, comma-separated set of
runtime capabilities without changing the public call signature. Lifecycle
and evaluation hooks request only the selectors they use. Direct output access
is explicit through `inject out`; ordinary output remains available through
terminating `return value` statements.

Lexical `let` bindings are immutable and lexical `var` bindings are mutable.
Neither becomes node state. Runtime collection iterators are borrowed views
confined to one evaluation and lower only through public hgraph view APIs.

The performance distinction is between the runtime topology produced by the
two paths. A graph definition itself flattens and does not remain as an
evaluation object. A generated runtime function can nevertheless fuse several
scalar operations into one node, eliminating intermediate primitive nodes,
bindings, scheduling, and change tracking when those intermediate ticks are
not part of the desired semantics.

## Compiler pipeline

All execution modes share one pipeline:

```text
source
  -> lexer and parser
  -> package target and module-closure resolution
  -> module descriptor and candidate-universe loading
  -> name and module resolution
  -> nominal operator binding and generic resolution
  -> canonical type resolution and temporal shape expansion
  -> function classification
  -> phase/effect checking and operator candidate resolution
  -> typed high-level IR
  -> hgraph semantic IR
  -> C++ source and build manifest
  -> native compiler and linker
  -> hgraph runtime
```

The frontend owns language diagnostics, lexical scope, public function
exposure, package membership, canonical types, function classification, phase
rules, type checking, and selection of a nominal operator identity through
local declarations, selective imports, or qualified module aliases. Every
operator-bound `fn` in the resolved target closure contributes a candidate.
Candidate selection within that identity delegates to the hgraph resolver; the
language project must not clone its matching or ranking rules.

The hgraph semantic IR is backend-neutral in representation but hgraph-specific
in meaning. It distinguishes wiring operations from evaluation operations and
retains source ranges for every declaration and expression.

## C++ backend contract

The first backend emits only public SDK constructs:

- Functions classified as composition become graph structs with `compose`
  methods and typed `Port` and `Scalar` parameters.
- Source `operator` declarations become deterministic nominal C++ markers;
  bound `fn` implementations register explicitly against those markers.
- Ordinary `export fn` declarations become public exact-callable entries;
  unexported exact functions remain module implementation details.
- Composition calls become public `wire` and operator-dispatch calls.
- Functions classified as runtime primitives become empty structs with typed
  static hooks.
- Runtime lifecycle and state become the corresponding hgraph selectors and
  hooks.
- Modules in the target closure contribute public headers, CMake packages and
  targets, descriptors, and explicit lifecycle and registration entry points.

Generated code must not construct runtime internals, depend on private headers,
or encode node indices and edges directly. Graph ordering, interning,
flattening, overload selection, and runtime planning remain hgraph concerns.

The application backend emits a deterministic module bootstrap. It initializes
providers in dependency order, records their keyed replayable installers, runs
those installers before graph wiring, and retains a registration handle for
each active module. Direct references from this bootstrap prevent provider
objects from being removed by static-library dead stripping.

Teardown first prevents new selection from a provider, then stops or rejects
live graphs holding provider leases, removes the provider's registrations and
installer intent, deinitializes it in reverse dependency order, and unloads its
native image only when no code or metadata reference remains. HGL module
lifecycle entry points are generated; they are not arbitrary source-level
initialization blocks.

Generated translation units use `#line` directives or equivalent source maps
so native compiler diagnostics refer to language source. A compiler error in
generated implementation detail is considered a compiler defect and should
include a retained generated-artifact path for diagnosis.

## Scripted and compiled execution

`hgl run` compiles a source package into a content-addressed cache and executes
it in a child process. `hgl repl` accumulates source declarations and evaluates
the current session through the same compile-and-run path. The initial REPL may
rebuild the complete session; correctness and diagnostic quality precede
incremental compilation.

Every mode derives the same candidate universe from the target and lock file.
When the REPL replaces a module, it removes the old registration handle before
activating the new revision and rebuilding dependent graphs; a registry reset
must not replay a removed provider.

`hgl build` emits a reproducible native build tree or installable application
artifact using the same typed IR and C++ backend. Debug and release profiles may
change optimization and retained diagnostics, never language semantics.

A future JIT is permitted only as another backend for the same typed semantic
IR. It must pass the scripted-versus-ahead-of-time parity suite before becoming
the default interactive engine.

## Project layout

The initial project owns:

- `src/` for the command and later compiler components;
- `docs/user-guide/` for observable language behavior and examples;
- `docs/developer-guide/` for grammar, lowering, tooling, and testing;
- `docs/design/` for architecture and language design records;
- `examples/` for provisional and later executable language programs;
- `tests/` for compiler, diagnostic, generated-code, and parity coverage.

Compiler components will be split into `syntax`, `semantics`, `ir`,
`codegen/cpp`, `driver`, and `repl` as each component acquires executable code.
Empty framework libraries are intentionally deferred.

## Compatibility axes

The language project will version these separately:

- source syntax and static semantics;
- language module descriptor format;
- compiled module lifecycle and registration ABI;
- generated-code requirements on the hgraph public SDK;
- cached artifact format;
- serialized hgraph values and record/replay data, which remain governed by
  hgraph and the owning extensions.

No source compatibility promise applies before the grammar reaches an accepted
language RFC.
