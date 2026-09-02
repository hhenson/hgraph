# Testing and compatibility

The language is complete only when source reaches observable hgraph behavior.
Parser-only milestones remain intermediate progress.

## Lexer and parser

Snapshots cover:

- `fn` declarations and anonymous functions;
- concise and block bodies;
- `let` and `var` declarations, assignment, and `for` iteration patterns;
- `const` parameters and defaults;
- `atomic<T>`, `set<T>`, `datetime`, and nested canonical types;
- contextual type keywords used as callable names, especially `map`;
- calls, indexing, named arguments, and expression precedence;
- tail expressions and explicit returns;
- state declarations and grouped inject declarations;
- `start`, ordered `when`, and `stop` blocks;
- complete and projected output assignment;
- source ranges, recovery, and multiple diagnostics.

Legacy draft spellings—`graph`, `node`, `ts<T>`, parameter-section semicolons,
`emit`, and endpoint metadata members—must not accidentally remain accepted
unless a later RFC deliberately reintroduces one.

## Canonical types and temporal shapes

Table-driven tests cover recursive expansion:

- scalar leaves;
- tuples, lists, sets, maps, and records;
- atomic container snapshots;
- atomic values nested inside structures;
- invalid `const atomic<T>` parameters;
- unsupported map keys and heterogeneous tuple mappings;
- descriptor round trips to public hgraph schemas.

Type diagnostics show both the canonical source type and expanded temporal
shape when that distinction explains the error.

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

- exact, generic, defaulted, named, lifted, ambiguous, and no-match calls;
- canonical source types expanded to matching hgraph schemas;
- `const` values participating in candidate selection;
- output schema inference;
- composition-time `key_set` resolving through the standard key projection;
- candidate ranking and rejection reasons;
- compiler prediction versus generated dispatch;
- descriptor/registry fingerprint mismatch.

## Generated C++

Golden tests are useful for canonical formatting, includes, identifier
escaping, source maps, and deterministic manifests. They do not replace a
native build.

End-to-end tests must:

1. generate into a fresh directory;
2. configure against an installed hgraph SDK;
3. use only public headers and imported targets;
4. compile the implementation kind selected from source;
5. execute through public evaluation APIs;
6. compare values, ticks, validity, deltas, and absence of ticks.

For each classified function, keep a minimal equivalent native C++ graph or
node and compare behavior at the same semantic level.

## Execution-mode parity

The same accepted example passes through:

- `hgl check`;
- `hgl emit-cpp` and a clean native build;
- `hgl run` on cold and warm caches;
- a REPL session;
- `hgl build` and its packaged artifact.

Compare semantic output rather than temporary paths. Failed compiles and
interrupted execution must not corrupt caches or the last valid REPL session.

## Documentation examples

Every `.hgl` file under `language/examples` begins as checked documentation.
As compiler slices land, promote each example through:

1. parser and type-shape coverage;
2. function classification;
3. typed IR;
4. generated C++ build;
5. native behavior;
6. scripted/AOT parity.

CI should eventually extract fenced `hgl` blocks and parse them by language
edition.

## Compatibility dimensions

Version independently:

| Dimension | Owner |
| --- | --- |
| Source grammar and semantics | language edition |
| Function classification rules | language edition |
| Module descriptor schema | descriptor-format version |
| Required hgraph public APIs | SDK constraint |
| Generated cache entries | cache format and complete cache key |
| Runtime values and record data | hgraph and owning extensions |

Before the first accepted RFC, source syntax remains provisional.
