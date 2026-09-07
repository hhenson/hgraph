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

Status: complete. Lexy materializes an HGL-owned, source-accurate arena with
missing, unexpected, and explicit invalid-line syntax. HGL-owned translation
produces diagnostics and structurally projects valid and recovered source into
the semantic AST. The former recursive-descent parser has been removed.

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

Status: complete for the source semantics currently defined. The resolved HIR
checkpoint owns the complete program in a
backend-independent arena and assigns stable identities to declarations,
parameters, locals, state, injectables, loop values, anonymous parameters,
types, imported operators, and intrinsics. Type completion then interns
context-neutral canonical source types, records complete local substitutions,
typed constants, exact and nominal call identities, wiring/runtime phases,
effects, capabilities, control flow, constraints, and source ranges. Concrete
imported calls are ranked by `OperatorRegistry`; calls awaiting wiring-time
values, callable erasure, or source-provider registration are explicitly
deferred rather than privately approximated. Every checked-in guide example
reaches `Module::completion == Typed`, `hgl check --dump-hir` is deterministic,
and a failed semantic pass cannot claim typed completion.

Typed HIR now owns one reusable generic-substitution engine and a separate
constraint solver. The solver orients positive-conjunction equalities to a
fixed point, evaluates closed type sets, `struct` categories, inherited field
reflection, Boolean composition, and nominal operator requirements, and uses
those facts while checking generic bodies. A declaration's requirements and an
implementation's substituted operator-contract requirements form an explicit
premise environment for nested constrained calls and constructions. Imported
viability queries delegate to `OperatorRegistry`; local viability accepts
exactly one applicable source implementation without inventing a ranking rule.
Local implementations are checked against, and inherit requirements from,
their local operator contract. Every implementation now also retains its
resolved nominal operator symbol, with imported defining-module identity kept
separate from native registry spelling.

Typed HIR also owns explicit `instantiate op<A, ...>` requests for local
generic operator implementations. It validates type/constant argument kinds,
candidate and contract constraints, and duplicate materializations before
retaining a classified substitution for every slot. `_` explicitly retains a
slot as a resolver variable; other arguments are concrete. Unrequested generic
implementation templates remain valid but are not candidates.

The defined source constraint language is closed: equality, membership, type
categories, structural reflection, nominal operator requirements, and Boolean
composition. An arbitrary residual `const` predicate has no agreed source or
execution semantics and remains a design question rather than invented Stage C
syntax. Imported operator-contract conformance and public native nominal-struct
metadata require the versioned descriptors owned by Stage F. Registry-backed
completion for deferred nominal calls occurs in hgraph IR once all wiring-time
values and erased callable shapes exist, and therefore belongs to Stage D.
Until those owning stages complete, the cases fail closed or remain explicitly
deferred; the temporary AST backends never silently discard a constraint.

- introduce stable symbol and declaration identities, canonical types, complete
  substitutions, typed constants, function kinds, phases, effects, and source
  ranges;
- move semantic validation out of backend walks;
- implement `hgl check --dump-hir` and HIR snapshot tests;
- make a successful semantic check produce complete HIR or fail closed.

Acceptance: all resolved guide examples produce typed HIR, invalid programs
leave the module unresolved, concrete native selection delegates to hgraph,
and HIR contains no emitted C++ or runtime wiring objects. Backend removal of
the temporary resolved-AST semantic walks belongs to stages D and E.

### D. Hgraph IR and direct wiring

Status: in progress. The first checkpoint introduces a dedicated
`hgl_graph_ir` target and lowers typed HIR into self-contained canonical type,
compile-time expression (including aggregate parameter defaults),
constraint, effective nominal-struct, operator-contract, and callable-interface
tables. Struct fields retain their defining contract and effective default;
requirements retain exact symbol and native registry identities without HIR
symbol or expression references.
`hgl check --dump-hgraph-ir` exposes that deterministic interface form. The
second checkpoint lowers all callable and test bodies into owned bindings,
values, resolved operations, substitutions, statements, blocks, and test
plans. It explicitly represents state, injectables, lifecycle, ordered
activation, traversal, assignment, returns, output and capability access, and
test evaluation. The module is now `Bodies`, not `Executable`. The direct
backend consumes that form and resolves against the active in-process registry;
schema-only native selection now copies its keyed provider identity through HIR
and hgraph IR without retaining a registry object. Hgraph IR also collects the
concrete keyed providers selected by non-deferred native operator calls into a
deterministic requirement inventory. An explicit execution-completion pass now
normalizes the package target's closed provider universe, rejects deferred or
unkeyed external operations and missing providers, verifies the inventory, and
advances successful modules to `Executable`. It stores only provider keys;
native provider handles and leases remain owned by hgraph registry resolution
and wiring plans. The driver does not invoke this pass until deferred operator
planning is implemented.

- [x] lower composition and runtime semantics into one explicit hgraph IR;
- [x] represent state, injectables, lifecycle, activation, validity, traversal,
  output, and semantic operator identities;
- [x] implement `hgl check --dump-hgraph-ir`;
- [x] attach concrete keyed-provider requirements;
- [x] validate the locked provider universe and advance eligible modules to
  `Executable`;
- [x] migrate direct wiring from `ResolvedModule` to hgraph IR, including
  canonical type materialization, lexical activation bindings, composition
  expansion, harness evaluation, entry execution, and driver-prepared settings.
- [x] preserve explicit reference schemas and reference-transparent
  compatibility through HIR, hgraph IR, and native type materialization.
- [x] classify traversal by its containing phase and directly expand
  independent `values` and `items` bodies over fixed temporal lists.
- [x] lower independent `values` and `items` bodies over maps and unbounded
  lists to native sink child graphs with explicit temporal captures.
- [x] retain explicit generic implementation materializations and their
  concrete/retained substitutions through HIR and hgraph IR.

Acceptance: direct-wiring behavior and diagnostics remain equivalent, and the
wiring target no longer includes syntax AST headers.

### E. C++ backend migration

- [x] make hgraph IR authoritative for module paths, callable identity,
  visibility and classification, operator identity, exports, and registration
  planning;
- [x] render callable and operator parameter/result types, names, roles, and
  selector signatures from hgraph IR;
- [x] render supported callable parameter defaults and omitted local-call
  arguments from hgraph IR;
- [x] emit nominal struct identity, abstractness, generic parameters, parents,
  and effective field layouts from hgraph IR;
- [x] migrate construction defaults from the temporary
  syntax/`ResolvedModule` adapter to hgraph IR;
- [x] migrate local `let`/`var` and state binding types from the temporary
  syntax/`ResolvedModule` adapter to hgraph IR;
- [x] derive internal callable dependency order and recursion diagnostics from
  reachable hgraph-IR body operations;
- [x] migrate concise composition bodies, including concise anonymous `map`
  functions, from the temporary syntax/`ResolvedModule` adapter to hgraph IR;
- [x] migrate composition block bodies from the temporary
  syntax/`ResolvedModule` adapter to hgraph IR;
- [x] migrate runtime-node bodies, lifecycle planning, activation analysis,
  traversal, and runtime lexical bindings from the temporary
  syntax/`ResolvedModule` adapter to hgraph IR;
- [x] migrate expression-level type syntax from the temporary
  syntax/`ResolvedModule` adapter to hgraph IR and remove the obsolete AST
  type/expression/call evaluator;
- [x] remove duplicate expression, type, generic, phase, and classification
  logic from the emitter;
- [x] retain typed hgraph-IR handles for structs, operators, callables, and
  tests in one source-order sequence, with source ranges owned by the
  referenced records;
- [x] replace the emitter's source-range-to-declaration association with those
  hgraph-IR source-order and source-map handles;
- [x] move scalar/operator enum spellings behind the HIR boundary and add an
  architecture test that rejects backend AST/resolver dependencies;
- [x] retain deterministic formatting, source maps, public-SDK code, and readable
  output;
- [x] remove the compatibility path by which a backend walks `ResolvedModule`.
- [x] emit explicit reference contracts and guarded fixed-list reference
  routing with selector-aware activation and validity analysis.
- [x] emit readable native `map_sink_` helpers for independent dynamic map and
  unbounded-list graph traversal, matching direct-wiring behavior.
- [x] emit one readable graph or node struct per requested generic
  implementation materialization, including retained fixed-list size markers,
  and register only those candidates.

Acceptance: both backends consume the same hgraph IR, existing generated tests
and installed consumers pass, and architecture tests reject backend-to-syntax
dependencies. The declaration, interface, callable-default, struct-layout,
construction-default, local-binding-type, dependency-order, and concise-body
checkpoints are complete, as are composition and runtime block-body emission.
Stage E is complete for the language surface implemented by the current
HGraph IR; unsupported language-depth items remain explicit roadmap work.

### F. Constrained native interface

- [x] choose and version a reviewable descriptor representation;
- [x] emit structured public/provider signatures, struct layouts, defaults,
  canonical types, and generic constraints;
- [x] read and validate one descriptor without loading native code or consulting
  the operator registry;
- [x] choose and version the public C-compatible lifecycle ABI and use it for
  scripted native module activation, replacement, and logical removal;
- [x] provide an installed native-package authoring API which emits, seals, and
  validates descriptors;
- [x] resolve exact canonical-value and overloaded generic collection-view
  native evaluation functions from explicit descriptors and emit direct
  readable calls in AOT modules;
- [x] parse top-level `native fn` C++ projections, select generic overloads by
  their type patterns, emit formatted plain `noexcept` functions, publish them
  in generated descriptors, and consume them from downstream HGL modules;
- [x] ship the compiled `hgraph.native` C++ substrate with `len` and
  `is_empty` over strings and the currently importable TSL/TSS/TSD/TSW views;
- generate normalized wrappers for C++ overloads, templates, exceptions, and
  ownership boundaries;
- [x] add phase, effect, ownership, dependent-lifetime, exception,
  thread-safety, build, lifecycle, and canonical fingerprint metadata;
- support owned opaque node state;
- prove descriptor-only checking and identical scripted/AOT behavior.

Acceptance is defined in [Native interface](native-interface.md#acceptance).
Raw pointers and callbacks in HGL contracts, implicit temporal lifting, native
C++ nested in graph/node bodies, and source-declared external dependencies
remain rejected.

### G. Standard-library migration

Status: the low-level `hgraph.native` substrate is compiled and installed; no
core graph or node implementation has been migrated yet.

- generate the complete core graph/node inventory and classify each item;
- select representative composition, stateless scalar-node, stateful-node,
  collection, and native-kernel migrations;
- close missing language semantics through explicit design discussions;
- replace implementations in dependency order while preserving public operator
  identities and Python/C++ behavior;
- remove each old implementation when its HGL replacement is accepted.

Acceptance is behavioral parity, generated-code inspection, installed-SDK
coverage, and performance evidence against the implementation removed.

## Feature status matrix (2026-09-07)

This table is the single status record for the language surface. Every
other status paragraph in `language/` links here instead of restating it.
The labels are the four of [Documentation](documentation.md#feature-status):
**implemented** (parsed, checked, lowered through every applicable backend,
behavior-tested), **partial** (the implemented subset and its fail-closed
boundary are stated), **provisional** (agreed syntax or semantics with no
accepted programs), and **blocked** (an unresolved language decision or a
missing public hgraph contract is named). Facts are from the review recorded
in [#767](https://github.com/hhenson/hgraph/issues/767); the diagnostics
quoted are the compiler's.

The compiler has crossed the gate for starting Stage G: the complete core
inventory can be generated and the first pure-composition candidates can be
selected. That does not mean the compiler is feature-complete or that every
core implementation is expressible; roughly 15% of `lib/std` is expressible
today (#767, "Readiness").

| Surface | Status | Fail-closed boundary or named blocker |
| --- | --- | --- |
| `module`, selective and aliased `use`, `export`, canonical JSON descriptors, generated registration | implemented | Only `hgraph.std`, `hgraph.analytics`, and modules named by `--module-descriptor` resolve; any other `use` is a `module` diagnostic. No wildcard imports or re-exports; dependency closure and lock files are not implemented. |
| `fn`, `export fn`, anonymous `fn`, bodyless `operator`, `impl fn` | partial | Both backends. `emit-cpp` rejects an `impl fn` of an imported operator; direct wiring reaches an `impl fn` only through a loaded native image and rejects a direct call; direct wiring rejects a call to a generic plain `fn` ("generic functions are not supported by the first pass"); a concise `map(..., fn(a) => ...)` lambda is lowered by `emit-cpp` but rejected by direct wiring ("anonymous functions are not supported by the first pass"), #767 item 3. |
| Generics and `requires` | partial | Closed constraint language (equality, membership, categories, reflection, nominal operator requirements, Boolean composition) in typed HIR. Residual `const` predicates, imported-contract conformance, native nominal-struct metadata, and source-candidate ranking fail closed; explicit generic arguments on calls are undefined. |
| `instantiate op<A, ...>` | partial for local contracts | Concrete arguments specialize a matching generic `impl fn`; `_` retains a resolver slot. Both IRs distinguish the cases, descriptors retain residual generics, and `emit-cpp` maps a retained fixed-list size to `SIZE<"name">`. Constraints over retained slots and retained values read by a body require residual-constraint/reification designs and fail closed. Materializing an `impl fn` of a selectively imported operator is blocked on descriptor-backed external contract metadata. |
| Canonical scalars, the eight temporal types, `@` and duration literals | partial | Lexer, parser, HIR, and both backends for `bool`, `i64`, `f64`, `str`, `date`, `time`, `datetime`, `duration`. Zoned and civil literals are rejected by both backends. Of the arithmetic table in the language reference only `str + str`, `duration ± duration`, `datetime ± duration`, `datetime - datetime`, and `duration * i64` are typed; `date ± duration`, `date - date`, `duration * f64`, and `duration / ...` are "arithmetic operands must both be numeric" (#767 item 2b: the emitter needs temporal arithmetic helpers before the checker admits them). |
| `zoned_time` scalar; `Time` and `CivilDateTime` ordering | blocked | hgraph-side asks recorded under Slice 2 with no RFC in `docs/source/rfc/` yet; both backends fail closed meanwhile. |
| `tuple`, `list`, `set`, `map` | partial | `list<T, n>`, `set<T>`, and `map<K, V>` map to TSL, TSS, and TSD. `eval` drives scalar and `atomic` parameters only; a structural `tuple` has no time-series schema in direct wiring; time-series tuple and list literals, and compound constant literals in generated defaults, are rejected. A fixed list size must be a positive constant (or a `const` generic of type `i64`), checked by typed HIR (`type: list size must be a positive constant or 'unbounded'`, PR #780); non-scalar map keys have no language rule yet (#767 item 6). |
| `atomic<T>` | implemented | Whether `atomic<f64>` is normalized to `f64` is not fixed (types-and-expressions.md). |
| `rolling<T, max[, min]>` | partial | Both backends for concrete tick-count and duration windows. Not accepted as a runtime-node parameter; window iteration and an either-kind parameter spelling are undefined; kind agreement and the size ranges (tick sizes positive, a duration minimum may be `0s`, no minimum above its maximum) are typed HIR diagnostics (PR #780). A `const` size generic may be concretely materialized; retained named rolling sizes remain blocked because hgraph has no corresponding named-size marker. Unresolved generic plain functions remain an `emit-cpp` limitation. |
| Named TSW size generics | blocked | `emit-cpp` binds a size-generic window to `TSWAny<T>`; hgraph's `TypePattern` carries concrete sizes and an any-window wildcard but no named size variable (Slice 2 ask, no RFC). |
| `ref<T>` | partial | Explicit contracts, descriptors, guarded fixed-list reference routing, and forwarded conditional captures in both backends. Wiring-time dereference, `map<K, ref<V>>`, and `ref<ref<T>>` are rejected. |
| `signal` | implemented | Input-only, payload-erased, no default value. |
| Nominal, generic, and abstract `struct`; defaults, optional fields, `delta<S>` | partial | Scalar Bundle values, type-only generic specializations, `atomic<S>`, field-wise temporal construction, and sparse scalar deltas in both backends. Constructor inference, multiple-parent field order, temporal `delta` consumption, nested temporal construction, and `delta` in a field default fail closed. |
| Explicit optional-field clearing (`field: null` in a `delta<S>`) | blocked | Needs a public hgraph clear-delta operation or encoding distinct from an omitted delta field; `ts_delta.h` has none ([Language model](language-model.md#structured-values-and-deltas)). |
| Typed `const` generic struct metadata (`Vector<T, const size>`) | blocked | hgraph nominal Bundle `generic_arguments` carry type arguments only; both backends report "const generic struct arguments require typed constant Bundle metadata in hgraph". |
| `const` parameters, `const` generics, `--set` | implemented | |
| `let`, `var`, typed uninitialized `var`, definite assignment | implemented | An assignment cannot change a `var`'s type; a runtime uninitialized local must be scalar. |
| Graph-phase `for`: `values` and `items` over fixed lists, independent bodies over maps and unbounded lists | partial | Both backends; `for` is phase-neutral. Graph-phase `keys`, predicates, scalar and `const` captures, sets, bundles, reductions, loop results, escaping assignments, and `return` fail closed; `for` in a `test` body is a `phase` diagnostic; dynamic-body tests are structure-only (#767 item 4). |
| Runtime `for`, `keys`/`values`/`items` with predicates, `key_set` | partial | Generated C++ only: the direct backend never evaluates a runtime body, so the scripted path is the C++ backend plus a loaded image. `key_set` inside a runtime body is rejected; unbounded-list added/removed views need a public hgraph view API. |
| `elements(list_or_set)` | provisional | Agreed 2026-09-06; `elements` is `name: unknown name 'elements'` today; retention of the `values` spelling is undecided (#767 item 6). |
| Runtime nodes | partial | Implemented, in generated C++ and scripted on Unix: activation from `modified`, variadic `valid`, ordered `when` handlers, `return`, scalar recordable `state` with an initializer, `inject out` (whole, prior, and keyed writes), `inject logger` (`info` only), one `start` and one `stop` block, passive sampled inputs, scalar/collection/rolling/ref/`signal` inputs. Fail closed: calls to other HGL functions, non-scalar state, zero-input sources, `key_set`, temporal inputs or `out` in lifecycle blocks, a runtime `if` used as a value. Declaration placement (`state`/`inject` before handlers, one `start` and `stop`, no nested `when`, no `out` or `return` in a lifecycle block) and the approved injectable list are `hgl check` diagnostics (PR #780); validity-dominance ordering is still checked by `emit-cpp` only. |
| `inject clock`, `inject scheduler` | provisional | Documented in the user guide; neither word occurs in `src/`; `emit-cpp` reports "injectable 'clock' is not supported by emit-cpp yet". |
| Scalar (wiring-time) `if`, including `else if` | implemented | |
| Temporal `if` | partial | Both backends: results, sinks, escaping and forwarded bindings, mixed results, omitted `else`, nested early-return continuations. Rejected: temporal `else if`, scalar captures in a branch, `return` from a branch that is not the function's return. The documented status of a temporal conditional embedded in another expression is under review (#767 items 4 and 5). |
| `switch` / `case` / `default` | provisional | Agreed 2026-09-06 ([Switch](switch.md)); no keyword, parser, or lowering; the words are not reserved, and a `switch` yields generic parse errors. |
| `enum`, `Mode::m`, `Mode(...)`, `keys`/`values`/`elements(Mode)` | provisional | Agreed ([Type extensions](type-extensions.md#enum-types)); `enum` is "expected a declaration, found 'enum'"; integer-conversion spelling and the native ABI are open. |
| `str(value)` | provisional | Agreed spelling; `str` is not an expression start ("expected an expression, found 'str'"). |
| `test`, `assert`, `eval` with dense sequences | partial | Direct wiring, after a scripted image when the unit has runtime functions. Accepted inputs: `eval` takes a module `fn` (a bare operator is rejected; wrap it in a `fn`), drives scalar and `atomic` parameters only (a structural `tuple`, `list`, `set`, `map`, or `rolling` parameter is rejected), needs at least one temporal input, and takes dense sequences only (timed sequences are provisional, below). An end bound and approximate comparison are undefined. |
| Timed harness sequences (`[0s: v, ...]`) | provisional | Parsed and typed; "timed sequences are not supported by the first pass". |
| `hgl run` | partial | `--entry`, `--mode`, `--start`, `--end`, `--set`; an entry is an `export fn` whose parameters are all `const`. |
| `hgl run --config run.toml` (`[run]`, `[run.params]`) | provisional | Documented format; not read. |
| Native interface | partial | JSON descriptor format v1, descriptor-only `hgl check`, `hgl::native_package`, lifecycle ABI v1 for scripted images, exact canonical-value plus overloaded generic collection-view calls, top-level source `native fn` C++ projections emitted as formatted plain functions and importable descriptor declarations, literal module-local `cpp include` declarations, and the installed `hgl::core_native` library/descriptor. Native `requires` clauses fail closed until catalog constraint reconstruction exists. Linked source dependencies, owned opaque state, scripted external dependencies, transitive closure, duration-window generics, nominal bundle/ref views, and the AOT lifecycle ABI remain; direct wiring does not emulate native C++. Format v1 labels every temporal parameter `"kind": "signal"`; renaming is an open v2 decision (#767 item 6). |
| Tooling: `check` (`--dump-tokens`, `--dump-ast`, `--dump-hir`, `--dump-hgraph-ir`), `test`, `run`, `emit-cpp`, `repl`, `hgl_add_module()` with `PYTHON_MODULE`, native cache v3 | partial | Scripted loading and the cache are Unix-only; Windows, child orchestration, cache pruning, and dependency lock files are staged; there is no `hgl build`; the driver does not invoke `hgraph_ir::complete`. |

The inventory comes next. Its first candidate set should prefer pure
composition and may identify representative stateless scalar nodes after
their actual requirements are recorded. Compiler work after that point is
driven by a selected migration and one already-defined semantic contract;
every `blocked` row stays fail-closed until its design or owning hgraph API
is agreed.

## Corrective programme (2026-09-07)

[#767](https://github.com/hhenson/hgraph/issues/767) reviewed the
specification against the compiler at `d523372a1` and fixed an ordered
corrective roadmap. Each item lands as one pull request referencing the
issue and updates the matrix above in the same change.

| # | Item | Status |
| --- | --- | --- |
| 1 | Parser: bound the applied-constructor look-ahead so comparisons parse | [PR #768](https://github.com/hhenson/hgraph/pull/768) |
| 2 | Checker enforcement: rolling, list, and map shape rules, the approved injectable list, runtime-block placement, and the temporal arithmetic table move into `type_check.cpp`; backend copies removed | open |
| 3 | Shared IR guards: `control_flow.cpp` diagnoses once, no diagnostic string duplicated across backends, one reserved-name table, concise `map` lambda parity | in flight |
| 4 | Tests: `hgl test` for every example with a `test`, the embedded temporal `if` pinned, direct-wiring refs, generics, and windows, dynamic-traversal ticks, REPL smoke gating, wired `stdlib/examples/invalid/` fixtures | in flight |
| 5 | Documentation reconciliation: this matrix, the corrected language reference, and the stale claims listed in the issue | in flight |
| 6 | Design records: error model, `i64` division, overflow, and NaN, string operators, first-tick validity, descriptor `kind` naming, `elements`/`values` alias policy, type-keyword callees, normative grammar; hgraph-side asks promoted to RFCs | open |
| 7 | Stage G on-ramp: the inventory, the five first candidates, then the ranked blockers | open |

## Prototype checkpoint (2026-09-06)

The prototype deliberately permits incompatible AST and implementation
changes while these slices are being exercised. What each surface does
today, its fail-closed boundary, and its named blockers are recorded once in
the [feature status matrix](#feature-status-matrix-2026-09-07); this section
no longer restates them. Slice numbering below still describes the intended
end-to-end acceptance rather than a claim that all earlier deliverables are
complete.

The C++ backend exists as a first pass: `hgl emit-cpp` lowers the same
composition subset the direct-wiring backend accepts, the runtime forms in
the matrix, and generic source `operator` / `impl fn` declarations to a
header/source pair, and `hgl_add_module()` builds it into a package with an
optional Python module.
On Unix, file-based `hgl test` and `hgl run` also compile a unit containing
runtime functions or implementations to a content-addressed image and load its
candidates into the command process before wiring.
Generated runtime sources and calls, compound constant literals, runtime-node
`if` used as a value, and runtime constructs outside the supported
selector/output forms fail closed with a diagnostic that names the construct.
A temporal conditional embedded inside another expression is implemented in
both backends and pinned by the parity fixture (`tests/codegen/parity.hgl`,
`choose_embedded`). The REPL edits lines with history
and completion on a terminal.

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
  duration `rolling<T, max_size[, min_size]>` and explicit `ref<T>` boundaries;
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
- `test` declarations, `assert`, `eval` with dense harness sequences and the
  `_` placeholder, and the sequence and tuple literals (implemented); timed
  harness sequences (provisional: parsed and typed, rejected by the harness);
- a textual typed-IR dump for tests and tooling;
- the direct-wiring backend for composition-only programs: constant
  folding, exact-function inlining, operator calls through hgraph's erased
  `wire_operator`, and `replay`/`record` harness wiring;
- `hgl check`, `hgl test`, `hgl run` with the command-line run configuration
  (implemented; the TOML `--config` file is provisional and not read), and a
  first `hgl repl` that rebuilds the session per input, with line editing,
  history and completion on a terminal;
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
  `eval_node` harness for the same call, dense and timed (not met: `eval`
  takes a module `fn` today, so an operator is wrapped in one, and only
  dense sequences run);
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
- explicit reference schemas and guarded fixed-list reference selection in
  generated runtime nodes;
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
