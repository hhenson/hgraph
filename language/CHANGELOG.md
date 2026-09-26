# Changelog

## Unreleased

- An operator implementation may extend its contract (runtime spec Wiring,
  WIR-22): after the contract's parameters it may declare more, each with a
  default that a call through the contract uses. The compiler used to reject
  any implementation whose parameter count differed from the contract's.
  Passing an extra parameter explicitly through the operator is not yet
  supported.
- Accept `map<K, ref<V>>` as a map containing references, lowered to
  `TSD[K, REF[V]]` with no reference around the map (owner ruling
  2026-09-26). The compiler previously rejected the form pending that
  ruling; the mapping recorded during the discussion,
  `REF[TSD[K, REF[V]]]`, is withdrawn. Nested `ref<ref<T>>` stays rejected.
- Infer a generic's type parameter from an argument with every `ref` removed,
  at every depth, as the runtime does (runtime spec Wiring, WIR-7 and
  WIR-14): `pass<T>(value: T)` given a `ref<f64>` binds `T` to `f64`, not
  `ref<f64>`. A `ref` in the parameter's own type binds beneath it; a call's
  expected result binds a whole-result type parameter no argument bound;
  contract conformance still compares declared types exactly. A program that
  was rejected because inference formed `ref<ref<T>>` (for example
  `wrap<T>(value: T) -> ref<T>` given a reference) now binds `T` beneath the
  reference and is accepted.
- Document recursive struct fields (ADR 0012): the user guide's "Recursive
  fields" section, and `examples/recursive-fields.hgl`, a linked list and a
  generic tree whose `test` blocks run under `hgl test` and again on the
  generated C++.
- Generate C++ for recursive struct fields (ADR 0012). An edge is an
  `hgraph::Edge<Target>` field, and a `TS<Edge<Target>>` endpoint in the
  temporal shape; generated structs register through hgraph's
  `recursive_bundle_closure` (hgraph RFC 0041), which the direct backend's
  type bridge now uses too, so both register the same schemas under the same
  names and agree tick for tick on the recursive fixture. The emitter defines
  each struct after the structs it holds inline and forward-declares only an
  edge target defined later. Generic specializations are named
  `Pair[int, str]` in both backends.
- Module descriptor format 6 (ADR 0004) adds a required `recursive` Boolean
  to every struct field, marking a recursive edge (ADR 0012) in an exported
  struct's layout. The reader checks that an edge is optional and an `atomic`
  type over a struct of the same module, and `hgl check` validates it without
  loading code. Descriptors in format 5 are rejected; rebuild a module to
  regenerate its descriptor.
- Realize recursive struct fields (ADR 0012) in direct wiring. An edge is an
  owner of its target, so a value is a finite tree compared, hashed and copied
  through its whole depth; structs that reach one another through edges
  register as one `recursive_bundles` batch per group of specializations, an
  edge to an abstract parent owns that parent's schema, and the temporal
  shape's edge is one `TS[Owned[T]]` endpoint that binds as `TS[T]`. `hgl
  test` constructs, compares and round-trips three-deep values through
  `eval`. The direct backend and its type bridge
  also find struct contracts and constructor fields through indexes rather
  than scans.
- Carry recursive struct edges (ADR 0012) through the passes both backends
  share. Typed HIR marks an admitted edge (`--dump-hir` prints ` recursive`),
  and hgraph IR marks it with its target's identity (`--dump-hgraph-ir` prints
  ` recursive->identity`). Every shared pass is shown to terminate on a
  recursive type, since none follows a field into its type; the stop moves
  from HIR lowering to hgraph-IR lowering, before the execution backends.
- Admit recursive struct fields at name resolution (ADR 0012). A field
  through which a value of a struct can contain another value of the same
  struct is a recursive edge; the resolver accepts it as an optional
  `atomic<T>` whose cycle runs through `T` and reports the rule any other
  edge breaks: a required edge or a replaced null default (rule 2), a missing
  atomic boundary with the fix spelled out (rule 3), a generic argument
  that wraps a parameter and so denotes an unbounded family of
  specializations (rule 4), and a cycle through a container or a generic
  argument (rule 8). Generic structs may otherwise join any cycle hgraph can
  register. A cycle through
  inheritance is rejected because hgraph cannot register it. Admitted edges
  stop at HIR lowering with a "not yet supported" diagnostic until the later
  passes realize them.
- Reject a struct field through which a value of the struct could contain
  another value of the same struct, by any path. Only a field naming its own
  struct was rejected before; a cycle through another struct of the module, a
  bare generic argument (`Box<Node>`) or an abstract parent's family passed
  `hgl check`, then crashed direct wiring with unbounded recursion, compared
  equal values as unequal, or emitted C++ that did not compile. The resolver
  now finds every such field in one pass over the module's struct references
  and reports each field of the cycle. A generic family is followed only to
  descendants that can be the field's specialization, so `inner: Event<f64>`
  inside `struct IntEvent: Event<i64>` stays valid, also when `IntEvent`
  reaches `Event` through a generic parent that passes its parameters
  through, such as `abstract struct Middle<T>: Event<T>`. Struct
  and constructor field names are looked up through an index rather than a
  scan per field.
- Check struct-heavy modules in time linear in their size. A constructor asked
  the constraint solver for each argument's field type, and each request
  rebuilt the struct's effective fields with a search per field: cubic in the
  field count (checking a module whose one constructor names 4,000 fields
  took 41 s). Effective fields are now
  built once per applied struct type with a name index. The type checker also
  scanned every type of the module for each declaration's generic struct
  applications, and the resolver scanned whole scopes for each name and copied
  the test overlay for every test declaration; types are now indexed by
  owning declaration and scopes are hashed. `hgl check` of a module with
  16,000 structs now takes 0.5 s, at a flat 32 us per struct from 4,000 up.
- An `atomic<S>` struct construction aggregates only the fields that have a
  value, so an omitted or `null` optional field stays unset instead of
  stopping the value from ever ticking. Generated C++ previously combined
  every field strictly and never published such a struct; direct wiring
  rejected any `atomic<S>` construction from ports. Both backends now build
  it the same way and agree tick for tick. The emitter also no longer reads a
  freed type while stripping `atomic<...>` from a constructor, which could
  report an unknown nominal type, and finds constructor arguments by name
  rather than scanning them once per field.
- Add `cache` declarations (ADR 0011): `cache name[: T] = init` is node-local
  data outside record/replay, declared like `state` and re-initialized on
  every start, lowered to the native `State<T>` selector. One scalar cache per
  runtime function, not beside `state`; both limits are hgraph's static-node
  contract and are reported as such. `hgraph.std` gains the parallel
  `schedule` source with native-parity tests, including the native
  positive-delay check in `start`.
- A source native whose parameters are all values is available in every node
  hook (`start`, `when`, `stop`), not only evaluation; a native taking a live
  input view stays evaluation-only. Descriptors record the phases, and
  imported natives may declare any subset of the node hook phases.
- Stop emitting `[[maybe_unused]]`. A new hgraph-IR reachability pass
  (`hgraph_ir::binding_uses`) tells the emitter which source bindings a hook
  reaches, and the emitter records the backend names it writes; a generated
  hook or `compose` signature names a parameter only when its body uses it
  and leaves the rest unnamed, and unreached state locals are not emitted. A
  `let` or `var` that is never read is now a diagnostic (dead code), and a
  loop whose body reads no element iterates without binding one. The `name`
  members of generated structs drop the attribute, and a module-internal
  composition no longer declares one.
- Repair strict MSVC builds of the HGL compiler and stage runtime DLLs beside
  Windows compiler/test executables and the installed compiler. Driver
  environment reads use the shared portable helper, and generated modules use
  MSVC large-object support. A Windows regression
  checks build-tree and installed compiler startup without developer DLL paths.
- Preserve concrete input schemas in generated positional and keyword pack
  calls, including lifted scalar constants. These calls previously attempted
  to narrow ports to an unresolved type variable and failed during wiring.
- Implement the `clock` and `scheduler` injectables (ADR 0010):
  `clock.evaluation_time()`/`now()`/`next_cycle_evaluation_time()`,
  `scheduler.schedule(delay[, on_wall_clock])`, `schedule_at`,
  `is_scheduled()` and `next_scheduled_time()`; the `scheduled()` handler
  selector, under which a handler adds no input to the activation set and a
  runtime function may have no temporal parameters at all; and the
  `passivate(input)`/`activate(input)` statements. `hgraph.std` gains parallel
  `take`, `freeze` and `until_true` with native-parity tests. The `schedule`
  operator remains blocked on non-recordable counter storage and start validation.
- Admit native functions that raise: `native fn ... throws` emits the C++
  body without `noexcept`, records the descriptor policy `translated`, and
  the reader accepts that policy in the evaluation phase. A raise ends the
  evaluation under hgraph's node error model, now the language rule
  (ADR 0009). `hgraph.native` binds the checked `power`, `shift_left`,
  `shift_right` kernels and a Python-slice `slice`; `hgraph.operators` gains
  parallel `pow_`, `lshift_`, `rshift_` and `substr` with native-parity
  tests, including the exceptions.
- Record the standard-library migration requirements ledger
  (`docs/design/migration-requirements.md`): the `HGL-MIG-001`–`015` and
  `HGL-LIB-001`–`004` identifiers the catalogue cites, each mapped to its
  catalogue blocker, accepted record and open decision. PR #801 is closed;
  the roadmap's corrective-programme table now links the merged PRs.
- Keep implementation-only constraints on `impl fn`: the executable HGL
  operator contracts no longer expose their candidates' native delegation
  requirements, and the guides distinguish public semantic constraints from
  algorithm dependencies.
- Use `#` for line comments and `/* ... */` for block comments, freeing `//`
  as the floor-division symbol backed by the fixed `floordiv_` operator.
- Add module-level `cpp include <header>` and `cpp include "header"` declarations
  for source-native C++ dependencies. Generated headers retain delimiter and
  first-use order, deduplicate repeats, and keep the metadata local to the
  defining HGL module.
- Bound the applied-constructor look-ahead to the tokens a generic-argument
  list can contain, so a `<` comparison followed anywhere later in the file by
  `> (` no longer breaks parsing (#767). The rule and its one residual
  ambiguity are recorded in the developer guide's "Struct construction".
- Enforce in typed HIR completion the rules the language reference already
  stated: rolling-window size kinds and ranges, positive fixed list sizes, the
  approved injectable list (`out` and `logger`; `clock` and `scheduler` agreed
  but not implemented), `out` requiring a function output, `state` and
  `inject` before the executable blocks, at most one `start` and `stop`, no
  nested `when`, and no `out` or `return` in a lifecycle block. `hgl check`
  now reports them; the backends' copies became internal assertions (#767
  item 2).
- Add an installed `hgl::native_package` C++ authoring API for exact scalar and
  package-declared nominal native signatures. It produces deterministic sealed
  descriptors and applies the same safety validator used by `hgl check`.
- Describe native types and declarations with phase, effect, ownership,
  borrowed-lifetime, exception, and thread-safety metadata. Seal descriptors
  with a canonical SHA-256 fingerprint and require the generated native module
  table to present the exact fingerprint before initialization.
- Add an installed C-compatible native module lifecycle ABI and move scripted
  image registration ownership behind its opaque, versioned module table.
- Lower explicit two-branch temporal `if` expressions through native
  `switch_` in both the direct and generated-C++ backends. A shared HGraph-IR
  pass computes branch captures and escaping effects; generated branches are
  readable graph structs, and scripted/AOT parity covers branch changes.
- Add strict reading and descriptor-only `hgl check` for versioned JSON module
  descriptors, including compatible unknown-member handling and diagnostics for
  duplicate keys, malformed records, unsupported versions, and dangling schema
  references. Descriptor validation remains independent of native loading and
  operator-registry state.
- Compile every checked-in language example through `hgl_add_module()`, adding
  generated support for nominal and generic structs, sparse structural deltas,
  generic operator implementations, fixed and duration windows, concise graph
  functions, logger injection, borrowed collection traversal and predicates,
  and keyed TSD output. Add native behavior coverage for the new forms and
  preserve nominal hierarchy and generic identity in public static schemas.
- Make `clang-format` a required final stage for every generated C++ module,
  emit public and private operator contracts as transparent aliases instead of
  derived marker classes, and give generated namespaces and internal contracts
  source-level names. Generated result functions also omit unreachable fallback
  throws after an unconditional return.
- Make generated operator registration return a provider handle, add targeted
  provider activation, and use those handles for transactional runtime-bearing
  REPL sessions. The native loader cache advances to v3; a replacement is
  compiled and loaded before the active provider is swapped, activation can
  restore the prior provider, and rejected declarations leave the prior
  session usable.
- Lower the first scalar runtime-function slice through `hgl emit-cpp` as
  native static nodes: activation from `modified`, variadic `valid` checks,
  ordered `when` handlers, replay-aware aggregate scalar state, `return`,
  `inject out`, state-and-configuration lifecycle blocks, passive sampled
  inputs, and ordinary policy for functions without `when`. Runtime exports and
  `impl fn`
  candidates register as node overloads; generated execution tests exercise
  their tick behavior through hgraph's public APIs.
- Add `hgl emit-cpp`: a composition-only module becomes a `<name>.h` /
  `<name>.cpp` pair of public hgraph authoring code in the module's namespace,
  with `--out-dir` or `--include-dir`/`--src-dir` placement, `--print`, and
  `--python` for a generated Python wrapper module. Exported functions and
  `impl fn` candidates register as hgraph operators under module-qualified
  names through a replayable installer.
- Add the `hgl_add_module()` CMake function (`cmake/HglLanguage.cmake`),
  which compiles `.hgl` sources with `emit-cpp` at build time into a library
  beside hand-written C++ and, with `PYTHON_MODULE`, a stable-ABI Python
  extension module plus generated wrappers. It replaces the planned `hgl build`.
- Give `hgl repl` line editing, history and tab completion on a terminal
  (isocline, behind `HGL_ENABLE_LINE_EDITING`); piped input is unchanged.
- Add the backend-parity fixture `tests/codegen/parity.hgl`, built through
  `hgl test` and through generated C++, and a Linux/macOS CI workflow for the
  toolchain.
- Keep inferred `var` types stable across assignment, reject invalid rolling
  sizes and zero constant divisors during C++ emission, produce valid Python
  aliases for keyword exports, and make installed and multi-config
  `hgl_add_module()` generation reproducible. Installed `hgl` also retains
  external dependency search paths so it can run from an SDK prefix.
- Add the `signal` input type: a payload-erased temporal parameter that
  activates on any tick of any type, accepted by both backends, descriptors,
  and the scripted runtime tests.
- Implement explicit `ref<T>` contracts: reference shapes are preserved
  through typed HIR, hgraph IR, native schema materialization, descriptors,
  generated C++, and guarded fixed-list reference routing; wiring-time
  dereference, `map<K, ref<V>>`, and nested references fail closed.
- Extend temporal `if` in both backends to outputless sink branches,
  escaping and forwarded bindings, mixed expression-and-assignment results
  through a generated structural bundle, an omitted `else` lowered onto a
  never-ticking `nothing` source, and early-return continuations planned once
  in shared HGraph IR for top-level and nested conditionals.
- Compile graph-phase iteration: `values` and `items` over fixed temporal
  lists expand at wiring time, and independent bodies over maps and unbounded
  lists become native per-key or per-index child graphs with broadcast
  captures; `for` no longer classifies a function as runtime.
- Emit exact canonical-scalar native calls in AOT modules from explicit
  module descriptors, checking the symbol, arity, parameter names, and types
  against the descriptor.
- Add `hgl check --dump-hir` and `--dump-hgraph-ir`, deterministic views of
  the typed HIR and the hgraph IR used by snapshot tests.
- Accept a typed `var` without an initializer and check definite assignment
  along every path, so a temporal conditional can assign an escaping result.
- Reconcile the documentation with the compiler (#767 item 5): one feature
  status matrix in the roadmap using the implemented, partial, provisional,
  and blocked labels; a corrected language reference (phase-neutral `for`,
  typed uninitialized `var` and literal productions in the grammar, the exact
  reserved-word list, no `hgl build`); runnable user-guide test and run
  examples with provisional forms labelled; a corrective-programme record;
  and the observed-but-undecided scalar, string, and validity behaviors
  listed as open decisions.
- Run `hgl test` in CTest over every guide example that declares a `test`
  block (configure-time discovery; runtime examples take the Unix-only
  scripted path), pin the expression-embedded temporal conditional in the
  parity fixture, add direct-wiring tick coverage for reference routing,
  generic window resolution and dynamic traversal, split the REPL smoke test
  into a composition-only session for every platform and the runtime session
  for Unix, and give the standard-library design fixtures `module` lines and
  a `# expect:` diagnostic convention with a CTest runner for the
  implemented definite-assignment fixture.
- Report each first-pass control-flow rule once from hgraph IR: the shared
  analysis attaches the rule to its plan (`PlanIssue`), lowering reports the
  context-free ones so `hgl check` rejects them, and the backends forward the
  rest instead of carrying copies; a CTest case now fails when both backends
  own the same diagnostic text. Optional-field clearing through a sparse delta
  has one wording. The direct backend wires `map(a, b, fn(x, y) => ...)` as a
  per-key child graph, matching the generated backend. `hgl emit-cpp
  --print-namespace` prints a module's C++ namespace and `hgl_add_module()`
  writes the Python bootstrap from the generated descriptors, removing the
  CMake copies of the reserved-name tables (#767, item 3).

## 0.1.0

- Establish the independently buildable language project and `hgl` command.
- Record the initial architecture, language model, module boundary, and roadmap.
- Add the initial user and developer guides, `fn` syntax, canonical temporal
  types, `const` wiring values, `atomic<T>` boundaries, C++ lowering contract,
  provisional runtime-function classification, state, grouped injectables,
  lifecycle blocks, ordered activation, output access, and examples.
- Specify nominal `struct` declarations, recursive temporalization with
  explicit `atomic<T>` boundaries, named construction, required/default/null
  optional fields, and contextual sparse `delta<S>` output values.
- Restrict struct inheritance to abstract data families, make concrete structs
  implicitly final, keep inherited field type and optionality invariant, and
  allow descendants to introduce or replace constructor defaults.
- Define invariant generic struct families over canonical value types and
  wiring-time constants, complete type application, constructor inference,
  struct constraints, and generic abstract-parent specialization.
- Implement prototype AST and parsing for structs, generic applications,
  `requires`, `null`, and `delta<S>`, plus semantic validation of
  abstract-only single inheritance, effective fields, construction, generic
  argument roles, and decidable closed requirements.
- Extend direct wiring with scalar/defaulted/inherited Bundle construction,
  type-only generic Bundle specializations, atomic struct harness values,
  sparse scalar deltas, and field-wise temporal struct composition; unsupported
  native-boundary cases fail with explicit diagnostics.
