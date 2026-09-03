# Changelog

## Unreleased

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
