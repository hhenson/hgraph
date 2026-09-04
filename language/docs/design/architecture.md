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
- Express nominal generic operator contracts whose `impl fn` implementations
  reuse hgraph candidate matching and ranking.
- Expose ordinary exact functions explicitly with `export fn`, while treating
  operator contracts and their bound implementation candidates as public by
  definition.
- Express abstract structured-data families with final concrete values and
  preserve their hierarchy through hgraph's native type realization.
- Express invariant generic struct families whose complete substitutions use
  the same constraint model as generic functions and operators.
- Make wiring-time and tick-time code visibly different and statically checked.
- Produce ordinary C++ that uses public hgraph authoring APIs.
- Offer source-first `check`, `run`, and REPL workflows for exploration.
- Produce reproducible ahead-of-time artifacts for deployment through the
  consumer's own CMake build (`hgl_add_module()`), not a second build tool.
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
  -> canonical source type and generic declaration resolution
  -> struct specialization and hierarchy resolution
  -> nominal operator binding and generic call resolution
  -> temporal shape expansion
  -> function classification
  -> phase/effect checking and operator candidate resolution
  -> typed high-level IR
  -> hgraph semantic IR
  -> direct wiring         (test, repl, run)   -> hgraph runtime, in process
  -> C++ source and build manifest -> native compiler -> hgraph runtime
```

The two bottom lines are the two backends of the same semantic IR. The
direct-wiring backend walks composition-phase IR and calls hgraph's public
wiring API; the C++ backend emits source. Which one a command uses is decided
by the program and the command, never by the language rules; the section
"Two backends, one wiring" below records the split.

The frontend owns language diagnostics, lexical scope, public declaration
exposure, package membership, canonical types and struct hierarchies, function
classification, phase rules, type checking, and selection of a nominal
operator identity through local declarations, selective imports, or qualified
module aliases. Every `impl fn` in the resolved target closure contributes a
candidate.
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
  `impl fn` implementations register explicitly against those markers.
- Ordinary `export fn` declarations become public exact-callable entries;
  unexported exact functions remain module implementation details.
- Struct declarations become nominal Bundle and recursively temporalized TSB
  schemas; fully applied generic specializations and abstract-family
  relationships are registered before graph wiring.
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

## Two backends, one wiring

A composition function is wiring-time code: every call in its body either
folds a constant or resolves an operator and asks hgraph to wire it. hgraph
exposes that step as a runtime API. Generated C++ reaches it through the typed
`wire<Operator>(...)` fronts, and the Python bridge reaches the same code
through the erased `wire_operator(Wiring&, name, args, ...)` entry in
`operator_dispatch.h`. Both paths run the same registry lookup, the same
resolver, and the same graph construction.

The direct-wiring backend uses the erased entry. It walks the semantic IR of a
composition function, evaluates constant expressions, and for each wiring
operation calls hgraph with the resolved operator name and the already-wired
argument ports. Test sequences and run inputs become hgraph's own replay and
record nodes (`replay_in_memory`, `dense_record`, `sparse_record`, the nodes
behind the C++ `eval_node` harness), and the graph then evaluates on the
unmodified hgraph runtime. Nothing in the language project runs at tick time.
The non-goal "reimplementing hgraph behavior in an interpreter" therefore
stands: the backend interprets wiring-time code only, and it interprets it
by calling hgraph.

Runtime functions are node bodies. They have no interpreted tick-time form and
need the C++ backend. The first scalar subset is emitted as native static nodes.
For file-based `test` and `run` on Unix, the driver compiles and loads that
emitted artifact before direct wiring reaches the function's module-qualified
operator identity. The backend still never evaluates the body itself.

The commands map onto the backends as follows:

- `hgl test`, `hgl repl`, and `hgl run` use the direct-wiring backend for a
  program whose evaluated closure is composition-only. They need the hgraph
  shared library and the descriptors in the lock file, not a native toolchain.
- `hgl emit-cpp` is the C++ backend's command. It writes the module as a
  header/source pair; building, linking and packaging that output is the
  consumer's CMake build, through the `hgl_add_module()` function the language
  installs (there is no `hgl build`: a second build tool would duplicate what
  CMake and the hgraph SDK already provide). File-based `hgl test` and
  `hgl run` use the same emitted C++ for the supported runtime subset through
  a cached Unix image; the REPL uses that image path when its accepted session
  contains runtime declarations.
- Both backends must build the same graph for every program both accept. The
  parity suite evaluates the test corpus through each backend and compares
  the recorded ticks; a divergence is a compiler defect, and the C++ backend
  is the reference. Today `tests/codegen/parity.hgl` is that corpus: `hgl test`
  runs its tests, and the same module, emitted and compiled through
  `hgl_add_module()`, is driven through hgraph's `eval_node` to the same
  ticks. `tests/codegen/runtime.hgl` separately exercises supported runtime
  functions through both the generated package and scripted compiled paths.

## Scripted and compiled execution

`hgl run` on a composition-only program wires and evaluates it in process.
The current Unix prototype compiles a unit with runtime functions to a
content-addressed native cache and loads it into that command process; the
image shares the process registry and remains resident until command exit.
The production route still needs portable child-process orchestration.
`hgl repl` rebuilds the complete runtime session transactionally through that
same cached image path. It compiles and loads a candidate before replacing the
active provider and restores the old provider if activation fails. Correctness
and diagnostic quality precede incremental compilation.

Every mode derives the same candidate universe from the target and lock file.
When the REPL replaces a module, it removes the old registration handle before
activating the new revision and rebuilding dependent graphs; a registry reset
must not replay a removed provider.

A package is a CMake project: `hgl_add_module()` runs `hgl emit-cpp` as a
build step, compiles the pair with any hand-written C++ into one library, and
with `PYTHON_MODULE` adds a stable-ABI nanobind module whose import registers
the package's operators, plus generated Python wrappers — the shape of every
hand-written hgraph extension, produced from HGL. Debug and release profiles
may change optimization and retained diagnostics, never language semantics.

A future JIT is permitted only as another backend for the same typed semantic
IR. It must pass the backend parity suite before becoming the default
interactive engine.

## Project layout

The initial project owns:

- `src/` for the command and later compiler components;
- `docs/user-guide/` for observable language behavior and examples;
- `docs/developer-guide/` for grammar, lowering, tooling, and testing;
- `docs/design/` for architecture and language design records;
- `examples/` for provisional and later executable language programs;
- `tests/` for compiler, diagnostic, generated-code, and parity coverage.

Compiler components are split into `syntax`, `semantics`, `wiring` (the
direct-wiring backend), `codegen` (the C++ backend) and `driver` (the commands
and the REPL); `ir` arrives when the two backends stop walking the resolved
tree directly. Empty framework libraries are intentionally deferred. The
`cmake/` directory holds the consumer-facing `HglLanguage.cmake` and the
Python-module template it configures.

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
