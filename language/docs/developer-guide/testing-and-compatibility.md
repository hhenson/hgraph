# Testing and compatibility

The language is complete only when source reaches observable hgraph behavior.
Parser-only milestones remain intermediate progress.

The lists below are the acceptance matrix, not a claim that every item has
landed. At the 2026-09-03 prototype checkpoint, syntax tests cover struct,
generic application, constraint, `null`, and delta nodes; resolver tests cover
effective single-parent fields, hierarchy failures, generic argument roles,
closed-set struct admission, nominal constraint binding, and constructor
shape; and direct-wiring tests cover scalar/defaulted/inherited Bundle values,
type-generic and atomic values, sparse deltas, and field-wise temporal
composition. The tests also lock fail-closed diagnostics for multiple parents,
typed `const` generic metadata, and explicit optional-field clearing.

## Lexer and parser

The frontend tests are Catch2 cases in `tests/syntax/` (`temporal_tests`,
`lexer_tests`, `parser_tests`; one `hgl_syntax_tests` binary registered as
the `hgraph_language_syntax` CTest case). They assert on token kinds and
values, on the `print_ast` dump of parsed snippets, and on the diagnostics
emitted for rejected input. Snapshots cover:

- bodyless `operator`, named `fn`, `export fn`, and anonymous `fn`
  declarations;
- nominal `struct`, `abstract struct`, `export struct`, and
  `export abstract struct` declarations with parent lists, newline-separated
  fields, inherited-default overrides, generic parameter lists, trailing
  `requires`, and `null` optional fields;
- type and `const` generic parameter lists;
- trailing `requires` clauses, type sets, categories, type equalities,
  reflection calls, and operator requirements;
- selective imports, module aliases, and `alias::declaration` references;
- concise and block bodies;
- `let` and `var` declarations, assignment, and `for` iteration patterns;
- `const` parameters and defaults;
- `atomic<T>`, `set<T>`, `rolling<T, max_size[, min_size]>`, the temporal
  scalars, and nested canonical types;
- `@` date, time, instant, civil, zoned, and zone literals and unit-suffixed
  duration literals, including omitted seconds and offset minutes, multi-part
  durations, the type selected by each shape, and rejection of non-calendar
  dates, `24:00`, zoned values without an offset, syntactically invalid zone
  names, fractional microseconds, exponents, out-of-order or repeated units,
  and unknown units, plus offset normalization and canonical re-spelling;
- contextual type keywords used as callable names, especially `map`;
- calls, indexing, named arguments, and expression precedence;
- named-only complete struct construction, fully applied generic constructors,
  inferred generic constructors, and contextual `delta<S>(...)` construction;
- tail expressions and explicit returns;
- state declarations and grouped inject declarations;
- `start`, ordered `when`, and `stop` blocks;
- complete and projected output assignment;
- `test` declarations, `assert`, `eval` with positional and named
  arguments, dense sequences with `_`, timed sequences with duration and
  datetime keys, sequence and tuple literals including the one-element
  tuple's trailing comma, and rejection of `test`, `assert`, `eval`, and a
  lone `_` as identifiers;
- source ranges, recovery, and multiple diagnostics.

Visibility cases include public `export struct` and `export abstract struct`,
and rejection of
`export operator`, `export use`, export on an anonymous function,
`export impl fn`, `impl` on an anonymous function, and `impl fn` with no
operator of that name in scope.

Legacy draft spellings—`graph`, `node`, `ts<T>`, parameter-section semicolons,
`emit`, and endpoint metadata members—must not accidentally remain accepted
unless a later RFC deliberately reintroduces one.

## Resolver

`tests/semantics/resolve_tests.cpp` (`hgl_semantics_tests`, CTest
`hgraph_language_semantics`) drives `resolve` with a table of registry
names in place of hgraph, so the suite stays hgraph-free. It covers the
kernel-name mapping of both modules and a module alias, the `module`
diagnostics for other modules and unknown exports, the scope chain
(locals, parameters, functions, imports, intrinsics, and shadowing),
unknown names, the phase rules of `assert`, `eval`, and `_`, function
classification from statement forms, `impl fn` binding and the
`fn`/operator clash, nominal structs and effective fields, generic applications
and decidable struct requirements, constructor validation, tests as non-values,
and that every guide example resolves.

## Canonical types and temporal shapes

Table-driven tests cover recursive expansion:

- scalar leaves;
- tuples, lists, sets, maps, and records;
- atomic container snapshots;
- atomic values nested inside structures;
- invalid `const atomic<T>` parameters;
- unsupported map keys;
- structural tuple expansion to positional un-named bundle fields;
- `list<T, n>` sizes, including the `unbounded` sentinel and rejection of
  non-positive literal sizes;
- descriptor round trips to public hgraph schemas;
- rolling-window default minimum normalization for both kinds, size
  validation (positive tick sizes, a positive duration maximum, a `0s`
  duration minimum, minimum not above maximum, rejection of mixed kinds),
  generic size binding, canonical identity (`rolling<f64, 5m>` is
  `rolling<f64, 300s>`), and exact tick `TSW` and registry `tsw_duration`
  schema identity;
- the temporal arithmetic table, including rejection of `datetime + datetime`,
  `time ± duration`, `zoned_time ± duration`, `i64 * duration`, ordering of
  zoned values, and cross-type comparison, constant folding that matches
  hgraph's whole-day `date` arithmetic and ties-to-even duration scaling, and
  no folding of zoned arithmetic, which needs the run's provider.

Type diagnostics show both the canonical source type and expanded temporal
shape when that distinction explains the error.

## Structured values and deltas

Semantic and generated-code tests cover:

- module-qualified nominal identity and distinct equal-shaped structs;
- invariant generic specialization identity by origin and complete type and
  constant argument list;
- rejection of bare origins, partial applications, generic parameter defaults,
  unresolved constructor inference, conflicting explicit/inferred bindings,
  `atomic` or `rolling` type arguments, and explicit generic application of an
  ordinary function or operator;
- constructor inference from named fields and expected type, including repeated
  variables and a fixed list binding a `const` size parameter;
- generic struct `requires` admission before schema registration;
- type-only generic Bundle metadata and pattern round trips through hgraph's
  public generic-origin support;
- fail-closed coverage for a `const` generic Bundle argument until native
  nominal metadata and patterns represent typed constants explicitly;
- abstract-only parent validation, hierarchy-cycle rejection, non-constructible
  abstract structs, implicitly final concrete structs, multiple abstract
  parents, and empty concrete leaves;
- inherited effective fields, invariant field types and optionality, accepted
  default addition/replacement, rejected default removal and typed field
  redeclaration, and `null` overrides only on optional fields;
- multiple-parent merging of compatible fields, explicit resolution of
  differing or one-sided defaults, and rejection of type or optionality
  conflicts;
- scalar Bundle, recursively temporalized TSB, and `atomic<S>` schemas from one
  declaration;
- closed scalar and atomic abstract-family values retaining their final leaf
  discriminator and using exactly the concrete descendants in the target
  module closure;
- temporal abstract base bundles and rejection of implicit derived-to-base
  temporal projection;
- generic abstract families partitioned by exact parent specialization,
  generic final children, fixed parent arguments, substituted inherited fields,
  and inherited defaults checked after substitution;
- nested atomic boundaries in structs, maps, and lists;
- named-only complete construction and rejection of missing required,
  duplicate, or unknown fields;
- ordinary defaults, `null` optional defaults, and rejection of `null` for a
  required field;
- scalar, temporal, and atomic construction contexts, including scalar lifting
  and the atomic aggregation activation/validity policy;
- immutable field access in scalar, temporal, and atomic contexts;
- sparse `delta<S>` construction with every field omittable and no defaults;
- recursive structural deltas and complete values at atomic boundaries;
- `return delta<S>(...)` and `out = delta<S>(...)`, including ordered
  last-write-wins behavior;
- rejection of `delta<S>` as a parameter, result, state, collection element, or
  struct field;
- harness and replay round trips that preserve omitted fields as no change;
- explicit `null` clearing of optional fields remaining distinct from omission
  through direct wiring, generated C++, record, and replay.

The last case is blocked until hgraph exposes a public mutation or canonical
delta encoding for explicit TSB field invalidation. A test must fail closed
until that contract exists; the compiler must not silently lower clear to the
existing typed-null no-change representation.

## Generic constraints and substitution

Table-driven semantic tests cover:

- independent `<U, V>` bindings accepting unrelated types;
- repeated `<U>` occurrences accepting equal source types and rejecting
  inconsistent re-binding;
- one source variable used across temporal and `const` contexts;
- closed-set membership such as `U in {f64, i64}`;
- type-category admission and rejection;
- Boolean composition and short-circuiting of pure constraints;
- structural field presence and missing-field diagnostics;
- `field_type` equalities resolving an output-only variable;
- expected output bindings validating the same equality in the opposite call
  context;
- fixed-point resolution of dependent equalities and diagnostics for cycles;
- nominal and qualified operator requirements;
- the same constraint IR admitting or rejecting a complete generic struct
  specialization without affecting overload ranking;
- compile-time rejection when a generic body uses an operation not proved by
  its signature or requirements;
- identical substitution and diagnostics in check, REPL, run, and build.

Constraint tests must distinguish matching, resolution, and admission. A
closed type set represented in `TypePattern` participates in ranking; a
residual `requires_` predicate only rejects. Include an overlap in which two
same-ranked admitted predicates remain ambiguous rather than selecting by
source or registration order.

When open structural patterns are implemented, test that required fields bind
their types, additional fields remain admissible, a more constrained pattern
outranks an unbounded fallback, and compiler prediction agrees with the native
hgraph resolver. Until then, a field-presence predicate must be described and
tested as admission-only.

## Function classification

Tests for the provisional source rule must prove:

- a body without node-only syntax becomes `CompositionFn`;
- `state`, `inject`, `start`, `when`, `stop`, or runtime collection iteration
  classifies the complete body as `RuntimeFn`;
- ambiguous or mixed forms fail with a `function-kind` diagnostic;
- declarations and lifecycle blocks obey their function-level ordering and
  cardinality rules;
- classification precedes kind-specific phase/effect checking;
- concise anonymous functions remain composition-only except when checked and
  inlined as runtime collection predicates;
- check, REPL, run, and build classify identical source identically.

Runtime semantic tests additionally cover:

- `modified(a, b)` activating when either input changes;
- `valid(a, b)` requiring both inputs to be valid;
- rejection of zero-argument `modified()` and `valid()` calls;
- top-level `valid(value)` versus recursive `all_valid(value)` semantics;
- statically admitted and unchecked-valid inputs;
- flow-sensitive payload reads guarded by `valid(input)`;
- activation union and common validity admission across ordered handlers;
- residual `when` predicates that cannot lower entirely to metadata;
- replay-aware aggregate state initialization and mutation;
- injectable resolution, duplicates, and phase restrictions;
- terminating returns, output fallthrough, last-write-wins, and accumulated
  collection deltas;
- invalid payload/output reads and incompatible output types.

Collection-view semantic tests additionally cover:

- phase-specific `key_set(tsd)` results in composition and runtime functions;
- `keys`, `values`, and `items` result arity and types for TSB, TSD, TSL, and
  TSS, including `i64` TSL indices;
- the absence of an `elements` alias;
- built-in `added`, `modified`, and `removed` predicates for every supported
  structure/traversal pair, plus diagnostics for unsupported pairs;
- built-in, named, and inline predicates, captures, short-circuit validity,
  and `last_modified`;
- rejection of predicate effects and iterator escape through locals, state,
  output, return, or captures;
- heterogeneous TSB static expansion and per-field diagnostics;
- fixed and unbounded TSL behavior, including native added/removed delta ranges;
- direct native filtered-range lowering versus generic loop-and-`if` behavior.

Local-binding tests distinguish immutable `let`, mutable `var`, and persistent
`state`, and prove that a runtime `var` is reinitialized for each execution.

## Operator resolution

Use a deterministic fixture and the real hgraph standard registry to cover:

- nominal operator identity by defining module and declaration name;
- an `impl fn` binding to a local operator or the one selectively imported
  operator;
- `impl fn` with no operator in scope, and a plain `fn` conflicting with an
  in-scope operator name;
- adding or removing an unrelated `use` never changing whether an existing
  `fn` is a candidate;
- rejection when two selective imports introduce different operators under one
  short name;
- two aliased modules exposing the same short operator name and resolving
  independently through qualified calls;
- aliased modules not creating implementation bindings;
- ordinary functions remaining exact without `impl`;
- operators and `impl fn` candidates being public without export modifiers;
- unexported exact functions remaining module-internal and `export fn` exact
  functions appearing in selective and qualified lookup;
- exported exact functions not forming overload sets;
- compatible concrete and generic implementation signatures, including type,
  rolling-size, and list-size variables, with `unbounded` binding a list-size
  generic;
- constrained variables, derived type substitutions, structural requirements,
  and required-operator capabilities;
- exact, generic, defaulted, named, lifted, ambiguous, and no-match calls;
- canonical source types expanded to matching hgraph schemas;
- `const` values participating in candidate selection;
- output schema inference;
- composition-time `key_set` resolving through the standard key projection;
- candidate ranking and rejection reasons;
- equal-ranked implementations of one operator remaining ambiguous regardless
  of declaration, import, or registration order;
- candidate discovery across every source and locked dependency module in a
  target, including provider modules with no imported exact declaration;
- installed but undeclared packages having no effect on the candidate set;
- one canonical operator import path regardless of provider count;
- compiler prediction versus generated dispatch;
- descriptor/registry fingerprint mismatch.

## Module lifecycle and registration ownership

Installed-SDK fixtures must compile at least two independent provider modules
and exercise their generated lifecycle entry points through the public hgraph
API. Tests cover:

- dependency-ordered initialization and reverse-ordered deinitialization;
- transactional rollback after partial initialization failure;
- idempotent repeated initialization and deinitialization;
- direct bootstrap references retaining static-library provider objects;
- registry reset replaying every active provider exactly once;
- provider removal deleting active candidates and installer intent;
- a later reset not resurrecting a removed provider;
- ambiguity and no-match diagnostics identifying provider modules;
- candidate-universe fingerprints agreeing between descriptors and runtime;
- deinitialization waiting or failing while a graph or plan retains a provider
  lease;
- logical removal retaining a native image safely when physical unloading is
  unavailable;
- REPL replacement removing the old revision's candidates before activating
  the new revision.

Failure injection must leave either the complete old module universe or the
complete new universe active, never a mixture. Module removal runs only at a
quiescent wiring boundary; tests must not rely on concurrent mutation of the
process-wide operator registry.

The hgraph core suite now covers the operator-owned subset of this matrix:
provider-specific candidate and installer removal, stale-handle isolation,
failed-installer rollback and retry, reset non-resurrection, and leases retained
by both reusable graph plans and runtime graph instances. Generated-runtime and
REPL tests additionally cover returned provider handles, logical removal, and a
failed candidate compilation leaving the previous session active. Multi-registry
transactions, dependency ordering, and activation-fault injection remain HGL
work.

## Direct-wiring backend and the harness

The first pass is covered by `tests/wiring/backend_tests.cpp`
(`hgl_wiring_tests`, CTest `hgraph_language_wiring`), which runs the
backend against the live registry: constant folding of every folded
operator, `eval` of a composition through the harness with `_`, defaults,
and literal conversion, the failure detail of a wrong value and of a wrong
length, selected tests and the REPL's described tail, the first-pass
limits as diagnostics (timed sequences, an unloaded runtime function, element types),
`run_program` into a stream with `--set`, `--start`, and a duration end,
and `format_time`. `hgraph_language_test_midpoint` runs `hgl test` over
the guide's `midpoint.hgl`.

The direct-wiring backend is tested against hgraph, not against a model of
it:

- for each standard operator used in the guides, `eval` over the operator
  records the same ticks as the C++ `eval_node` harness in
  `hgraph/lib/testing/eval_node.h` for the same arguments, dense and timed,
  including padding, never-ticking outputs, and outputless callees;
- exact-function inlining wires the same graph as calling the operators
  directly (compare the wired node and edge sets, not only the ticks);
- `const` folding produces the scalar arguments hgraph's resolver lifts,
  including temporal constants and `[run.params]` values of every mapped
  TOML type;
- a runtime function without a loaded native image is an operator diagnostic
  naming its module-qualified identity; the file driver loads the supported
  runtime subset before invoking this backend;
- `hgl test` reports each failing assertion with the cycle or time, the
  expected element, and the observed element, and exits non-zero;
- `hgl run` in simulation over an entry with `[run.params]` prints the same
  ticks as hgraph's `run_graph` for the equivalent graph, and the
  command-line overrides win over the file.

Every `test` in the guide examples runs under `hgl test` in CI.

`hgraph_language_test_runtime` and `hgraph_language_run_runtime` compile and
load the runtime fixture through the actual CLI, covering an exported node, a
private runtime helper, a source-defined operator implementation, lifecycle and
state code in the compiled unit, and a const-only entry whose graph contains a
runtime node. `hgraph_language_native_compile_failure` injects a missing
compiler, checks the diagnostic and retained artifact directory, then removes
the test artifact.

`hgraph_language_native_cache` exercises the file-command cache as a black box:
cold miss, warm hit, digest-corrupted entry quarantine and repair, compiler
identity isolation, source invalidation, and two simultaneous cold publishers
converging on one complete entry without leaked staging directories.

## Generated C++

Golden tests are useful for canonical formatting, includes, identifier
escaping, source maps, and deterministic manifests. They do not replace a
native build.

The first pass has both. `tests/codegen/emitter_tests.cpp` checks what the
emitter prints (hgraph-free, over a table of kernel names like the resolver
tests): the namespace, the markers and their selectors, declarations and
out-of-line definitions, the folding rules, `const` wiring at a temporal
parameter, marker and named-argument spelling, helper ordering, keyword
escaping, Python keyword aliases and collisions, fixed-type `var` assignment,
zero-divisor and rolling-size diagnostics, the Python wrapper, determinism,
and every fail-closed diagnostic.
`tests/codegen/generated_tests.cpp` then compiles `tests/codegen/parity.hgl`
through `hgl_add_module()` under the repository's warnings and evaluates the
generated graphs with `eval_node` — directly by struct and by registry name,
where the `const` defaults apply — asserting the ticks the module's own
`test` blocks assert. The same generated target compiles every checked-in HGL
example. `generated_structural_tests.cpp`, `generated_generic_tests.cpp`, and
`generated_example_tests.cpp` exercise nominal hierarchy and generic metadata,
sparse structural deltas, fixed and duration window schemas, generic operator
resolution, collection predicates and iteration, and keyed collection output.
`tests/codegen/runtime.hgl` and
`generated_runtime_tests.cpp` exercise the compiled node path: modified-or and
valid-and predicates, passive sampling, ordered state mutation and final-write
behavior, selector metadata, prior-output access, lifecycle configuration, and
ordinary no-`when` policy. The command itself is checked on both a composition
and a runtime fixture.
The CMake package test configures the installed-helper path without an `hgl`
target, proves that touching the compiler regenerates outputs, checks keyword
module namespace escaping, and asserts multi-config-safe native-module output.
Acceptance also installs the SDK to a fresh prefix and builds a small
`hgl_add_module()` consumer against it. That proves the installed compiler can
resolve external shared dependencies before generating and compiling the
consumer module. The language CI also launches `hgl` from a fresh Runtime
component install on Linux and macOS.

End-to-end tests must:

1. generate into a fresh directory;
2. configure against an installed hgraph SDK;
3. use only public headers and imported targets;
4. compile the implementation kind selected from source;
5. generate and invoke the complete module registration bootstrap;
6. execute through public evaluation APIs;
7. compare values, ticks, validity, deltas, and absence of ticks;
8. deinitialize the module set and prove removed candidates stay removed after
   registry reset.

For each classified function, keep a minimal equivalent native C++ graph or
node and compare behavior at the same semantic level.

## Backend parity

The same accepted example passes through:

- `hgl check`;
- `hgl test` on the direct-wiring backend;
- `hgl emit-cpp` and a clean native build through `hgl_add_module()`, with
  the same tests run through the generated code;
- `hgl run` on both backends, cold and warm caches;
- a REPL session;
- the packaged artifact a consumer's CMake produces from the emitted code.

A tick recorded by one backend and not the other, or a differing value,
is a compiler defect; the generated C++ is the reference.

Compare semantic output rather than temporary paths. Failed compiles and
interrupted execution must not corrupt caches or the last valid REPL session.

## Documentation examples

Every `.hgl` file under `language/examples` begins as checked documentation.
As compiler slices land, promote each example through:

1. parser and type-shape coverage;
2. function classification;
3. typed IR;
4. direct-wiring behaviour through `hgl test`;
5. generated C++ build;
6. native behavior;
7. backend parity.

CI should eventually extract fenced `hgl` blocks and parse them by language
edition.

## Compatibility dimensions

Version independently:

| Dimension | Owner |
| --- | --- |
| Source grammar and semantics | language edition |
| Function classification rules | language edition |
| Module descriptor schema | descriptor-format version |
| Compiled module lifecycle ABI | module-ABI version |
| Required hgraph public APIs | SDK constraint |
| Generated cache entries | cache format and complete cache key |
| Runtime values and record data | hgraph and owning extensions |

Before the first accepted RFC, source syntax remains provisional.
