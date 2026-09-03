# Changelog

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
