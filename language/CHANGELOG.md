# Changelog

## Unreleased

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
