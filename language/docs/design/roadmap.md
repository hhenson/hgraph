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
- a provisional example kept out of executable tests.

Acceptance:

- the command builds with repository warnings enabled;
- help and version smoke tests pass;
- default hgraph builds remain independent of the language project;
- the core source distribution excludes `language/`.

## Slice 1: atomic frontend

Deliverables:

- lexer, parser, source manager, and structured diagnostics;
- `module`, `use`, stateless `node`, and `graph` declarations;
- `bool`, `i64`, `f64`, `str`, `ts<T>`, and `signal` types;
- name resolution, graph/node phase checking, and exact user-declaration type
  checking;
- a textual typed-IR dump for tests and tooling;
- `hgl check`.

Acceptance:

- parser and diagnostic snapshot tests cover valid and invalid programs;
- graph value reads and node wiring calls fail as phase errors;
- all AST and IR nodes retain precise source ranges;
- malformed input recovers sufficiently to report multiple useful errors.

## Slice 2: C++ vertical slice

Deliverables:

- C++ lowering for stateless atomic compute nodes and graphs;
- hgraph kernel module descriptor;
- imported operator resolution through the hgraph resolver;
- generated CMake build manifest and source mapping;
- `hgl emit-cpp` and `hgl build`.

Acceptance:

- end-to-end tests compile generated code against an installed hgraph SDK;
- graph and node behavior is asserted through public hgraph evaluation APIs;
- generated code uses no private hgraph headers or runtime internals;
- no-match and ambiguity diagnostics retain hgraph candidate reasons;
- generated output is deterministic for identical inputs.

## Slice 3: scripted workflow

Deliverables:

- content-addressed native build cache;
- `hgl run` in an isolated child process;
- REPL sessions that accumulate declarations and rebuild through the same
  backend;
- testing sources and sinks suitable for exploration without defining native
  adaptors in the language.

Acceptance:

- the same source produces identical ticks in `run`, REPL, and ahead-of-time
  execution;
- failed compilation or execution cannot corrupt a later REPL session;
- cache keys cover compiler, hgraph, extension, profile, and target inputs;
- diagnostics map to original source in every mode.

## Slice 4: language depth

Candidates, in risk order:

- temporal scalars, enums, records, and structural time-series types;
- recordable state and explicit ephemeral cache;
- lifecycle blocks with restricted capabilities;
- higher-order graph operations and runtime control flow;
- generic declarations and user-defined operator overloads;
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
- debug and release profiles have equivalent semantics;
- cache and lock formats are versioned;
- source compatibility and language edition policy are documented;
- deployment artifacts do not require the compiler or source tree at runtime;
- performance evidence shows generated node hot paths are comparable to the
  equivalent hand-authored C++ implementations.
