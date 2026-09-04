# Roadmap

Status: initial design

## Prototype checkpoint (2026-09-04)

The prototype deliberately permits incompatible AST and implementation
changes while these slices are being exercised. The current tree implements
the struct, constraint, and construction syntax described below; resolves
abstract-only single inheritance, effective fields, constructor completeness,
generic argument roles, and statically decidable closed requirements; and
directly wires scalar Bundle values, `atomic<S>` values, type-only generic
specializations, sparse scalar deltas, and simple field-wise temporal structs.
The generated C++ backend also lowers the first scalar runtime-node subset:
ordered activation, aggregate recordable state, direct and terminating output,
and lifecycle hooks over state and `const` configuration.

The implementation fails closed where the public or language contract is not
settled: multiple-parent field order, constructor inference, typed `const`
generic Bundle metadata, explicit optional-field clearing, temporal deltas,
callable generic substitution, replaceable runtime images, portable
scripted loading, and runtime collection or generic lowering. Slice numbering
below still describes the intended end-to-end acceptance rather than a claim
that all earlier deliverables are complete.

The C++ backend exists as a first pass: `hgl emit-cpp` lowers the same
composition subset the direct-wiring backend accepts, the first scalar runtime
node subset, and non-generic source `operator` / `impl fn` declarations to a
header/source pair, and `hgl_add_module()` builds it into a package with an
optional Python module.
On Unix, file-based `hgl test` and `hgl run` also compile a unit containing
runtime functions or implementations to a content-addressed image and load its
candidates into the command process before wiring.
Structs, generics, duration rolling windows (no compile-time hgraph marker
yet), compound constant literals, `if` as a value, and runtime constructs
outside the scalar subset fail closed with a diagnostic that names the
construct. The REPL edits lines with history and completion on a terminal.

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
- generated markers and explicit registration for source-defined operator
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
- public hgraph TSW patterns that match a concrete duration window and bind
  named maximum and minimum size generics of either kind, and a compile-time
  duration `TSW` marker (the parity matrix records the gap);
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
- `hgl emit-cpp` (done for the composition subset and first scalar runtime-node
  subset) and the
  `hgl_add_module()` CMake function that builds packages, including the Python
  extension module and wrappers (done); there is no `hgl build`;
- the backend parity suite: every `hgl test` the direct-wiring backend
  accepts is also run through generated C++ and must record the same ticks
  (seeded by `tests/codegen/parity.hgl`; the examples follow as they become
  emittable).

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

Status: the cached Unix file-command layer is implemented. `hgl test`
and `hgl run` emit a unit containing runtime functions or implementations,
build or reuse a complete content-addressed native image, load it into the
command process's registry, and run through the ordinary wiring backend. Cache
publication is atomic, damaged entries are quarantined, and compile failures
retain and report their artifacts. Windows support, child orchestration, cache
pruning, and replaceable REPL images remain. The underlying hgraph registry now
has removable provider handles and graph-plan leases; generated HGL lifecycle
and transactional replacement do not yet consume them.

Deliverables:

- content-addressed native build cache (implemented on Unix);
- portable `hgl run` and `hgl test` child orchestration for programs with
  runtime functions;
- REPL sessions that accumulate declarations and rebuild through either
  backend as the session's classification requires;
- transactional replacement of generated module registration handles without
  stale candidate or installer state;
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
