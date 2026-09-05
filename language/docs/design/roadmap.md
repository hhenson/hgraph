# Roadmap

Status: active staged delivery

## North-star outcome

The target is to author the graph and node implementations currently supplied
by hgraph core as HGL standard-library modules. HGL is an ahead-of-time source
language: those modules transpile to reviewed C++ implementations and are
compiled into the distribution. The shipped implementation, runtime semantics,
and first-class native authoring surface therefore remain C++. The migration
leaves a smaller hand-written C++ runtime kernel containing graph execution,
storage, type and operator resolution, public authoring contracts, approved
injected capabilities, and native primitives which generated C++ calls. It does
not make hgraph core depend on the HGL compiler at runtime.

Production distributions build standard-library HGL ahead of time and ship its
generated, formatted C++ and compiled artifacts. Generated sources are retained
as inspectable implementation inputs; they use the same public native contracts
as hand-written C++ and do not create an HGL runtime. Python and C++ compatibility
surfaces continue to expose the public hgraph operators and resolve to those
registered C++ implementations. During compiler incubation, the compiler and
generated standard library remain a parallel downstream consumer of the public
hgraph SDK; promotion into core is a deliberate build-time migration rather than
a reversal of the C++-first runtime boundary.

Every current graph and node implementation enters a migration inventory. An
implementation may remain in C++ only when it is explicitly classified as one
of:

- a runtime bootstrap or storage/execution primitive that HGL lowers onto;
- an adaptor, callback, thread, or external-resource owner outside HGL's
  deliberate language boundary;
- an approved native scalar or opaque-state primitive exposed through the
  constrained native descriptor;
- a temporarily blocked migration with the missing HGL semantic or public
  hgraph contract named.

The classification is not permission to leave ordinary algorithmic nodes in
C++. The standard-library migration is complete only when the inventory has no
unclassified implementation and every non-migrated item has a reviewed kernel
reason.

Undefined language behavior is a valid stopping point. The implementation must
not invent syntax, ownership, phase, delta, or generic semantics merely to move
an inventory item. Such an item becomes a focused design question with examples
and stays fail-closed until resolved.

## Compiler architecture stack

Before additional language breadth, the prototype is moved onto the architecture
in [Compiler architecture](compiler-architecture.md). The stack is ordered so
each pull request is independently reviewable and later changes do not hide
semantic movement inside a parser or backend rewrite.

### A. Architecture and documentation

- record the declarative-parser, shared-IR, and native-descriptor decisions;
- define source, syntax, HIR, hgraph IR, backend, and module boundaries;
- define documentation audiences and executable status rules;
- reconcile stale implementation-status claims;
- record this core-library migration programme.

Acceptance: documentation links resolve, no source syntax is invented, and the
current versus target architecture is explicit.

### B. Parser evaluation and migration

Status: lexy selected and the token grammar runs in conformance mode; the
source-accurate arena, AST projection, and hand-written-parser removal remain.

- build representative grammar spikes for the shortlisted C++ parser tools;
- measure valid parsing, multi-error recovery, source fidelity, grammar
  readability, debug/release compile cost, and portability;
- record the selected implementation and evidence in ADR 0001;
- replace the parser behind the existing syntax result before changing later
  pass behavior;
- preserve unexpected and missing syntax for diagnostics and tooling.

Acceptance: the complete syntax suite and guide corpus pass, malformed-source
snapshots report at least the previous useful diagnostics, and parser-library
headers remain private to the syntax implementation.

### C. Typed HIR

- introduce stable symbol and declaration identities, canonical types, complete
  substitutions, typed constants, function kinds, phases, effects, and source
  ranges;
- move semantic validation out of backend walks;
- implement `hgl check --dump-hir` and HIR snapshot tests;
- make a successful semantic check produce complete HIR or fail closed.

Acceptance: all resolved guide examples produce HIR, invalid programs stop
before lowering, and the HIR contains no emitted C++ or runtime wiring objects.

### D. Hgraph IR and direct wiring

- lower composition and runtime semantics into one explicit hgraph IR;
- represent state, injectables, lifecycle, activation, validity, traversal,
  output, operator identities, and provider requirements;
- implement `hgl check --dump-hgraph-ir`;
- migrate direct wiring from `ResolvedModule` to hgraph IR.

Acceptance: direct-wiring behavior and diagnostics remain equivalent, and the
wiring target no longer includes syntax AST headers.

### E. C++ backend migration

- migrate C++ generation to hgraph IR;
- remove duplicate name, type, generic, phase, and classification logic from
  the emitter;
- retain deterministic formatting, source maps, public-SDK code, and readable
  output;
- remove the compatibility path by which a backend walks `ResolvedModule`.

Acceptance: both backends consume the same hgraph IR, existing generated tests
and installed consumers pass, and architecture tests reject backend-to-syntax
dependencies.

### F. Constrained native interface

- choose and version a reviewable descriptor representation and lifecycle ABI;
- provide a native-package authoring API which emits descriptors and normalized
  wrappers;
- add phase, effect, ownership, exception, build, and fingerprint metadata;
- support a canonical scalar evaluation function and owned opaque node state;
- prove descriptor-only checking and identical scripted/AOT behavior.

Acceptance is defined in [Native interface](native-interface.md#acceptance).
Raw pointers, callbacks, implicit temporal lifting, and arbitrary C++ source
remain rejected.

### G. Standard-library migration

- generate the complete core graph/node inventory and classify each item;
- select representative composition, stateless scalar-node, stateful-node,
  collection, and native-kernel migrations;
- close missing language semantics through explicit design discussions;
- replace implementations in dependency order while preserving public operator
  identities and Python/C++ behavior;
- remove each old implementation when its HGL replacement is accepted.

Acceptance is behavioral parity, generated-code inspection, installed-SDK
coverage, and performance evidence against the implementation removed.

## Prototype checkpoint (2026-09-05)

The prototype deliberately permits incompatible AST and implementation
changes while these slices are being exercised. The current tree implements
the struct, constraint, and construction syntax described below; resolves
abstract-only single inheritance, effective fields, constructor completeness,
generic argument roles, and statically decidable closed requirements; and
directly wires scalar Bundle values, `atomic<S>` values, type-only generic
specializations, sparse scalar deltas, and simple field-wise temporal structs.
The generated C++ backend lowers every checked-in example: ordered activation,
aggregate scalar recordable state, direct, prior-value, and keyed TSD output,
logger injection, lifecycle hooks over state and `const` configuration,
nominal/generic structs and sparse deltas, generic operators, fixed and duration
windows, concise `map` functions, and borrowed runtime collection iteration.
Generated headers and sources are mandatory `clang-format` output and public
operator contracts are transparent aliases rather than derived marker classes.

The implementation fails closed where the public or language contract is not
settled: multiple-parent field order, constructor inference, typed `const`
generic Bundle metadata, explicit optional-field clearing, consumption of
temporal deltas, general callable substitution, portable scripted loading, and
runtime calls. Slice numbering
below still describes the intended end-to-end acceptance rather than a claim
that all earlier deliverables are complete.

The C++ backend exists as a first pass: `hgl emit-cpp` lowers the same
composition subset the direct-wiring backend accepts, the runtime forms above,
and generic source `operator` / `impl fn` declarations to a
header/source pair, and `hgl_add_module()` builds it into a package with an
optional Python module.
On Unix, file-based `hgl test` and `hgl run` also compile a unit containing
runtime functions or implementations to a content-addressed image and load its
candidates into the command process before wiring.
Generated runtime sources and calls, compound constant literals, `if` as a
value, and runtime constructs outside the supported selector/output forms fail
closed with a diagnostic that names the construct. The REPL edits lines with
history and completion on a terminal.

Development proceeds through executable vertical slices. Parser-only progress
is not a usable milestone: each language slice must reach hgraph wiring,
runtime behavior, and diagnostics. Slice 1 reaches them through the
direct-wiring backend, which wires composition-only programs through hgraph's
public erased dispatch and runs them in process; Slice 2 adds generated C++
for everything else. The two backends and their split are recorded in
[Architecture](architecture.md#two-backends-one-wiring).

## Slice 0: project scaffold

Deliverables:

- independent `language/` CMake project;
- opt-in repository build;
- installable `hgl` command with help and version reporting;
- architecture, language model, module, and roadmap records;
- user and developer guide foundations;
- provisional examples kept out of executable tests.

Acceptance:

- the command builds with repository warnings enabled;
- help and version smoke tests pass;
- default hgraph builds remain independent of the language project;
- the core source distribution excludes `language/`.

## Slice 1: atomic frontend and direct wiring

Deliverables:

- lexer, parser, source manager, and structured diagnostics;
- `module`, selective and aliased `use`, bodyless `operator`, named `fn`,
  `export fn`, and anonymous `fn` syntax;
- `bool`, `i64`, `f64`, `str`, `date`, `time`, `datetime`, `duration`,
  `civil_datetime`, `timezone`, `zoned_datetime`, `zoned_time`, tuple, sized
  and unbounded list, set, map, and `atomic<T>` types, plus tick-count and
  duration `rolling<T, max_size[, min_size]>`;
- `@` temporal literals with RFC 9557 zone annotations and unit-suffixed
  duration literals, validated and normalized in the lexer;
- type and `const` generic declarations, nominal operator identities, and
  explicit `impl fn` implementation binding;
- `requires` clauses with closed type sets, categories, type equality,
  structural reflection, and nominal operator requirements;
- automatically public operators and candidates, plus explicit public exposure
  for ordinary exact functions;
- `const` wiring parameters and recursive temporal-shape expansion;
- immutable `let`, mutable lexical `var`, and runtime collection `for` loops;
- provisional runtime classification from `state`, `inject`, lifecycle, and
  `when` syntax, including mixed-form diagnostics;
- grouped inject declarations, ordered activation blocks, state aggregation,
  and output access grammar;
- `key_set`, `keys`, `values`, and `items`, including built-in, named, and
  inline traversal predicates;
- name resolution and kind-specific phase checking;
- `test` declarations, `assert`, `eval` with dense and timed harness
  sequences and the `_` placeholder, and the sequence and tuple literals;
- a textual typed-IR dump for tests and tooling;
- the direct-wiring backend for composition-only programs: constant
  folding, exact-function inlining, operator calls through hgraph's erased
  `wire_operator`, and `replay`/`record` harness wiring;
- `hgl check`, `hgl test`, `hgl run` with the command-line and TOML run
  configuration, and a first `hgl repl` that rebuilds the session per
  input, with line editing, history and completion on a terminal;
- parser-check all first-slice guide examples and run their tests.

Acceptance:

- parser and diagnostic snapshot tests cover valid and invalid programs;
- ambiguous function-kind syntax fails before lowering;
- canonical and atomic shapes map to public hgraph schemas;
- nominal operator identities and generic rolling-window size bindings survive
  into typed IR, while concrete rolling windows map to hgraph schemas;
- independent and repeated type variables, derived equality substitutions, and
  residual admission predicates remain distinct in typed IR;
- private exact functions are absent from module interfaces, exported exact
  functions are present, and operator candidates carry provider identity;
- all AST and IR nodes retain precise source ranges;
- malformed input recovers sufficiently to report multiple useful errors;
- `eval` over a standard operator records the same ticks as the C++
  `eval_node` harness for the same call, dense and timed;
- a runtime function without a loaded candidate is rejected by the
  direct-wiring backend with a diagnostic that names its operator identity;
- `hgl run` of a composition-only entry produces the same ticks in
  simulation as the equivalent hgraph `run_graph` call.

## Slice 2: C++ vertical slice

Deliverables:

- C++ lowering for composition functions and runtime functions with aggregate
  state, approved injectables, lifecycle hooks, ordered activation, and output;
- public-view lowering for metadata and collection iteration, including native
  delta ranges and heterogeneous TSB expansion;
- hgraph kernel module descriptor;
- source and imported nominal operator resolution through the hgraph resolver;
- transparent contract aliases and explicit registration for source-defined operator
  implementations;
- package-target and locked-dependency candidate-universe construction,
  independent of source imports and without declaration re-exports;
- generated module descriptors, initialization/bootstrap entry points,
  replayable installers, registration handles, and reverse-order
  deinitialization;
- public hgraph provider-scoped operator candidate provenance, installer and
  candidate removal, failed-install rollback, and live-plan lease support
  (implemented); registration ownership for the remaining module surfaces is
  still required;
- public hgraph TSW patterns that bind named maximum and minimum size generics
  of either kind (the wildcard and compile-time duration marker are
  implemented);
- standard-library ordering overloads for `Time` and `CivilDateTime`, so the
  language's temporal operation table is hgraph's;
- a `ZonedTime` core scalar (`CivilTime` plus `ZoneId`, registered as
  `zoned_time`) with `date + zoned_time -> zoned_datetime` (raising on a
  repeated or skipped time, the `Reject` policies), a policy-taking
  `resolve(date, zoned_time, ...)`, accessors,
  JSON and Arrow codecs, and a Python wrapper, as an amendment to RFC 0002;
- public source-type resolution bindings for context-neutral HGL
  generics, plus open structural patterns for required-field matching;
- source mapping (`#line` or a sidecar map; the first pass writes source
  comments) and the module descriptor for generated packages;
- `hgl emit-cpp` (done for every checked-in example) and the
  `hgl_add_module()` CMake function that builds packages, including the Python
  extension module and wrappers (done); there is no `hgl build`;
- the backend parity suite: every `hgl test` the direct-wiring backend
  accepts is also run through generated C++ and must record the same ticks
  (seeded by `tests/codegen/parity.hgl`; every checked-in example is now
  generated and compiled, with focused native behavior coverage for the newly
  supported forms).

The hgraph-side requirements above are tracked here while the language design
is still moving. Once agreed they are promoted to an RFC in
`docs/source/rfc/` before the corresponding core changes land.

Acceptance:

- end-to-end tests compile generated code against an installed hgraph SDK;
- classified function behavior is asserted through public hgraph evaluation
  APIs;
- generated code uses no private hgraph headers or runtime internals;
- named rolling-window size generics bind and resolve through public hgraph
  patterns;
- constrained and derived generic substitutions agree between compiler
  prediction and hgraph dispatch, and structural predicates never acquire an
  implicit declaration-order tie-break;
- every provider in the locked target is linked and registered before wiring,
  and descriptor/runtime candidate fingerprints agree;
- removing a provider prevents future selection and registry reset cannot
  restore its candidates;
- deinitialization refuses or waits on live graph leases and never unloads code
  still referenced by a plan;
- no-match and ambiguity diagnostics retain hgraph candidate reasons;
- generated output is deterministic for identical inputs.

## Slice 3: scripted workflow

Status: the cached Unix command layer is implemented. `hgl test`, `hgl run`,
and runtime-bearing REPL sessions emit a unit, build or reuse a complete
content-addressed native image, load it into the command process's registry,
and run through the ordinary wiring backend. Cache publication is atomic,
damaged entries are quarantined, and compile failures retain and report their
artifacts. Generated modules return removable provider handles and REPL
replacement stages the new image before swapping providers, restoring the old
one if activation fails. Windows support, child orchestration, cache pruning,
and a transaction spanning non-operator module surfaces remain.

Deliverables:

- content-addressed native build cache (implemented on Unix);
- portable `hgl run` and `hgl test` child orchestration for programs with
  runtime functions;
- REPL sessions that accumulate declarations and rebuild through either
  backend as the session's classification requires (implemented on Unix);
- transactional replacement of generated operator registration handles without
  stale candidate or installer state (implemented at the quiescent REPL
  boundary);
- testing sources and sinks suitable for exploration without defining native
  adaptors in the language.

Acceptance:

- the same source produces identical ticks in direct wiring, child-process
  `run`, REPL, and ahead-of-time execution;
- failed compilation or execution cannot corrupt a later REPL session;
- failed module replacement leaves the prior active module universe intact;
- cache keys cover compiler, hgraph, extension, profile, and target inputs;
- diagnostics map to original source in every mode.

## Slice 4: language depth

Candidates, in risk order:

- complete the implemented nominal `struct` prototype with nested temporal
  construction, runtime consumption of contextual `delta<S>` values, and
  atomic aggregation validity semantics;
- complete the implemented abstract-only single-inheritance checks with
  scalar/atomic closed-family registration after settling multiple-parent
  field order and the temporal base-projection spelling;
- complete the implemented invariant type-generic struct origins with typed
  `const` arguments, constructor inference, full `requires` evaluation, and
  exact-specialization abstract families;
- extend public nominal Bundle metadata and generic patterns with typed constant
  arguments rather than encoding a `const` specialization only in its name;
- add a public native operation or canonical delta encoding for explicitly
  clearing an optional TSB field without confusing it with an omitted delta;
- enums and additional canonical temporal structures;
- explicit ephemeral cache semantics;
- additional lifecycle capabilities and output access;
- higher-order functions and runtime control flow;
- explicit generic arguments on function/operator calls, generic parameter
  defaults, partial generic type application, and cross-module implementation
  coherence beyond the initial constraint model;
- the rolling-window runtime iteration surface and a parameter spelling that
  accepts either window kind;
- an explicit end bound and approximate comparison for `eval`, and delta
  spellings for set, map, and list harness elements;
- incremental compilation or a JIT backend.

Each capability must map to a first-class public C++ hgraph path and have
native generated-code behavior tests. User-defined overloads must reuse the
hgraph registry rather than add language-local dispatch.

## Core standard-library migration programme

The first task is an automatically maintained inventory of public operator
contracts and their graph/node candidates. It records implementation kind,
source location, generic signature, state and injectable use, dependent native
libraries, Python exposure, behavior tests, benchmarks, and migration status.

Migration proceeds by increasing semantic demand:

1. **Composition graphs.** Move pure topology first. This validates imports,
   exact helpers, generics, and operator binding without adding runtime
   semantics.
2. **Stateless scalar nodes.** Move nodes expressible with activation, validity,
   and a terminating result. Compare fused generated code with the hand-written
   static node.
3. **Stateful and lifecycle nodes.** Exercise recordable state, startup,
   ordered activation, prior output, injectables, and deterministic teardown.
4. **Collections and windows.** Exercise borrowed delta views, keyed/list/set
   mutation, dynamic shapes, rolling storage, and output mutation.
5. **Native-library algorithms.** Keep the algorithm in a reviewed C++ library
   where appropriate and express its hgraph lifecycle and activation in HGL
   through the constrained native interface.
6. **Sources, sinks, services, and adaptors.** Express graph-facing policy and
   lifecycle in HGL where the approved capability model permits it. Keep
   callback, thread, queue, protocol, and external-resource ownership in native
   providers unless a later language decision deliberately expands the
   boundary.

For each migrated candidate:

- the operator contract and overload ranking are unchanged;
- native C++, generated HGL, and Python compatibility behavior are compared at
  the public wiring level;
- tick sequences, validity, delta behavior, exceptions, lifecycle, teardown,
  and replay are covered as applicable;
- generated C++ is reviewed for clarity and contains no private runtime access;
- hot paths have performance and allocation evidence;
- the prior implementation is removed rather than retained as an unselected
  duplicate;
- any required native primitive is independently useful and has a public,
  reviewed contract rather than exposing the old node wholesale.

The first migration set is chosen only after the inventory exists. The
roadmap does not name source syntax for a blocked capability in advance.

## Production and release gates

Before the language is described as production-ready:

- the backend parity suite passes on supported platforms;
- generated applications build against an installed SDK, not only the
  repository tree;
- module descriptors cover at least one independently packaged extension;
- module initialization, reset replay, deinitialization, registration removal,
  and safe retained-image behavior pass installed-SDK lifecycle tests;
- debug and release profiles have equivalent semantics;
- cache and lock formats are versioned;
- source compatibility and language edition policy are documented;
- deployment artifacts do not require the compiler or source tree at runtime;
- performance evidence shows generated node hot paths are comparable to the
  equivalent hand-authored C++ implementations.
