# Language examples

These files illustrate the proposed first language slice. Each one is a
CTest case that must pass `hgl check`, and `midpoint.hgl` also runs its
`test` under `hgl test`. The other examples declare no tests yet: some use
runtime functions, generic functions, or `impl fn` declarations that the
direct-wiring backend checks but does not run. Structural values and
type-generic struct specializations also have executable unit coverage.

- [`midpoint.hgl`](midpoint.hgl) uses an internal helper, `export fn`, an
  atomic tuple, a `const` window, and a `test` of the unexported helper.
- [`runtime-choice.hgl`](runtime-choice.hgl) contrasts a wiring-time topology
  choice, explicit time-series selection, and a `when` runtime function.
- [`stateful-node.hgl`](stateful-node.hgl) demonstrates aggregate state,
  grouped injectables, lifecycle blocks, ordered handlers, previous output,
  and incremental collection output.
- [`collection-views.hgl`](collection-views.hgl) demonstrates dual-phase
  `key_set`, runtime `keys`/`values`/`items`, built-in and inline predicates,
  `last_modified`, and mutable lexical `var`.
- [`operators-and-generics.hgl`](operators-and-generics.hgl) demonstrates a
  nominal bodyless `operator`, a generic `impl fn` implementation, const-generic
  rolling-window sizes, an exported exact function, the default minimum window
  size, and a duration window. Operators, functions, and structs may now carry
  the agreed `requires` constraint syntax.
- [`structural-types.hgl`](structural-types.hgl) demonstrates a recursively
  temporal struct, `atomic<S>`, a type-generic struct, closed-set requirements,
  abstract-only inheritance with a default override, and a sparse delta in a
  runtime function, alongside temporal maps and an anonymous `fn`.
- [`conditional-result.hgl`](conditional-result.hgl),
  [`conditional-results.hgl`](conditional-results.hgl), and
  [`conditional-mixed-results.hgl`](conditional-mixed-results.hgl) exercise
  temporal branch results, escaping assignments, structural result packing,
  and remapping in both compiler backends.
- [`conditional-forwarding.hgl`](conditional-forwarding.hgl) preserves an
  initialized result through an implicit or explicit unassigned branch and
  exercises independent reference forwarding for structural result fields.
- [`conditional-omitted-else.hgl`](conditional-omitted-else.hgl) shows that a
  consumed temporal conditional without `else` produces no tick while false,
  using a type-resolved `nothing` branch in both compiler backends.

As compiler slices land, each example should advance from parsing and typed IR
coverage through `hgl test` to generated C++ behavior and backend parity.
The CTest suite checks every example, and its generated fixtures compile the
implemented AOT surface under the repository warning policy. The
backend-parity module that is built both ways lives in
`../tests/codegen/parity.hgl`.
The acceptance
sequence is defined in the
[Developer Guide](../docs/developer-guide/testing-and-compatibility.md#documentation-examples).
