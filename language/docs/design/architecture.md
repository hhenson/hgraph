# Architecture

Status: initial design

## Purpose

The hgraph language is a domain-specific authoring language for typed hgraph
graphs and nodes. It gives graph authors a compact, learnable surface while
preserving the C++ hgraph runtime as the source of truth.

The project is hosted beside hgraph while the design matures, but it is an
independent consumer of the public hgraph SDK. It must remain possible to move
the directory into a separate repository without changing hgraph core.

## Goals

- Express composable graphs, compute nodes, and calls to registered operators.
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
- A general C or C++ FFI available to graph authors.
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
are selected by imports in a program or package manifest.

## Semantic phases

The language has two user-code execution phases and one native integration
boundary:

| Construct | Phase | May do | Must not do |
| --- | --- | --- | --- |
| `graph` | Wiring | Compose nodes and graphs, pass ports, inspect scalar build parameters, select fixed topology | Read time-series values, keep runtime state, perform runtime side effects |
| `node` | Evaluation | Read input views, update declared state, emit a tick | Add graph topology, resolve overloads, acquire arbitrary external resources |
| imported adaptor or service | Native C++ | Own callbacks, threads, queues, protocols, and resources through hgraph lifecycle contracts | Expose unrestricted native execution to language source |

Graph bodies flatten through hgraph wiring. They are not runtime evaluation
objects. Node bodies lower to typed static C++ node hooks; their implementation
objects remain empty and instance data is represented by hgraph selectors and
plans.

## Compiler pipeline

All execution modes share one pipeline:

```text
source
  -> lexer and parser
  -> name and module resolution
  -> phase and effect checking
  -> type checking and imported operator resolution
  -> typed high-level IR
  -> hgraph semantic IR
  -> C++ source and build manifest
  -> native compiler and linker
  -> hgraph runtime
```

The frontend owns language diagnostics, lexical scope, phase rules, and type
checking. Imported hgraph operator selection delegates to the hgraph resolver;
the language project must not clone its candidate matching or ranking rules.

The hgraph semantic IR is backend-neutral in representation but hgraph-specific
in meaning. It distinguishes wiring operations from evaluation operations and
retains source ranges for every declaration and expression.

## C++ backend contract

The first backend emits only public SDK constructs:

- Graph declarations become graph structs with `compose` methods and typed
  `Port` and `Scalar` parameters.
- Graph calls become public `wire` and operator-dispatch calls.
- Compute nodes become empty structs with typed static `eval` hooks.
- Node lifecycle and state become the corresponding hgraph selectors and
  hooks.
- Imported modules contribute public headers, CMake packages and targets, and
  explicit registration calls.

Generated code must not construct runtime internals, depend on private headers,
or encode node indices and edges directly. Graph ordering, interning,
flattening, overload selection, and runtime planning remain hgraph concerns.

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

`hgl build` emits a reproducible native build tree or installable application
artifact using the same typed IR and C++ backend. Debug and release profiles may
change optimization and retained diagnostics, never language semantics.

A future JIT is permitted only as another backend for the same typed semantic
IR. It must pass the scripted-versus-ahead-of-time parity suite before becoming
the default interactive engine.

## Project layout

The initial project owns:

- `src/` for the command and later compiler components;
- `docs/` for architecture and language design records;
- `examples/` for provisional and later executable language programs;
- `tests/` for compiler, diagnostic, generated-code, and parity coverage.

Compiler components will be split into `syntax`, `semantics`, `ir`,
`codegen/cpp`, `driver`, and `repl` as each component acquires executable code.
Empty framework libraries are intentionally deferred.

## Compatibility axes

The language project will version these separately:

- source syntax and static semantics;
- language module descriptor format;
- generated-code requirements on the hgraph public SDK;
- cached artifact format;
- serialized hgraph values and record/replay data, which remain governed by
  hgraph and the owning extensions.

No source compatibility promise applies before the grammar reaches an accepted
language RFC.
