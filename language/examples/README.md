# Language examples

These files illustrate the proposed first language slice. The current `hgl`
scaffold does not parse them yet, so they are documentation inputs rather than
executable tests.

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
  rolling-window sizes, an exported exact function, the default minimum
  window size, and a duration window.
- [`structural-types.hgl`](structural-types.hgl) contrasts recursively temporal
  maps with an atomic tuple and uses an anonymous `fn`.

As compiler slices land, each example should advance from parsing and typed IR
coverage through `hgl test` to generated C++ behavior and backend parity.
The acceptance
sequence is defined in the
[Developer Guide](../docs/developer-guide/testing-and-compatibility.md#documentation-examples).
