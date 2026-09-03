# Roadmap

Status: initial design

Development proceeds through executable vertical slices. Parser-only progress
is not a usable milestone: each language slice must reach generated C++, hgraph
wiring, runtime behavior, and diagnostics.

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

## Slice 1: atomic frontend

Deliverables:

- lexer, parser, source manager, and structured diagnostics;
- `module`, selective and aliased `use`, bodyless `operator`, named `fn`,
  `export fn`, and anonymous `fn` syntax;
- `bool`, `i64`, `f64`, `str`, `datetime`, tuple, sized and unbounded list,
  set, map, and `atomic<T>` types, plus tick-count
  `rolling<T, max_size[, min_size]>`;
- type and `const` generic declarations, nominal operator identities, and
  explicit `impl fn` implementation binding;
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
- a textual typed-IR dump for tests and tooling;
- `hgl check`;
- parser-check all first-slice guide examples.

Acceptance:

- parser and diagnostic snapshot tests cover valid and invalid programs;
- ambiguous function-kind syntax fails before lowering;
- canonical and atomic shapes map to public hgraph schemas;
- nominal operator identities and generic rolling-window size bindings survive
  into typed IR, while concrete rolling windows map to hgraph schemas;
- private exact functions are absent from module interfaces, exported exact
  functions are present, and operator candidates carry provider identity;
- all AST and IR nodes retain precise source ranges;
- malformed input recovers sufficiently to report multiple useful errors.

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
- public hgraph provider-scoped candidate provenance, installer removal,
  registration removal, and live-plan lease support;
- public hgraph TSW patterns that bind named maximum and minimum size generics;
- generated CMake build manifest and source mapping;
- `hgl emit-cpp` and `hgl build`.

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
- every provider in the locked target is linked and registered before wiring,
  and descriptor/runtime candidate fingerprints agree;
- removing a provider prevents future selection and registry reset cannot
  restore its candidates;
- deinitialization refuses or waits on live graph leases and never unloads code
  still referenced by a plan;
- no-match and ambiguity diagnostics retain hgraph candidate reasons;
- generated output is deterministic for identical inputs.

## Slice 3: scripted workflow

Deliverables:

- content-addressed native build cache;
- `hgl run` in an isolated child process;
- REPL sessions that accumulate declarations and rebuild through the same
  backend;
- transactional replacement of generated module registration handles without
  stale candidate or installer state;
- testing sources and sinks suitable for exploration without defining native
  adaptors in the language.

Acceptance:

- the same source produces identical ticks in `run`, REPL, and ahead-of-time
  execution;
- failed compilation or execution cannot corrupt a later REPL session;
- failed module replacement leaves the prior active module universe intact;
- cache keys cover compiler, hgraph, extension, profile, and target inputs;
- diagnostics map to original source in every mode.

## Slice 4: language depth

Candidates, in risk order:

- enums, records, and additional canonical temporal structures;
- explicit ephemeral cache semantics;
- additional lifecycle capabilities and output access;
- higher-order functions and runtime control flow;
- generic constraints, explicit generic arguments, output-directed inference,
  and cross-module implementation coherence;
- duration rolling windows and their runtime iteration surface;
- incremental compilation or a JIT backend.

Each capability must map to a first-class public C++ hgraph path and have
native generated-code behavior tests. User-defined overloads must reuse the
hgraph registry rather than add language-local dispatch.

## Production and release gates

Before the language is described as production-ready:

- scripted and AOT parity suites pass on supported platforms;
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
