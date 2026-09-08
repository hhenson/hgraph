# Language examples

These files illustrate the first language slice and are executable
documentation (developer guide, "Documentation examples"). Every example is a
CTest case that must pass `hgl check`, and every example that declares a
`test` block also runs under `hgl test`: `../tests/CMakeLists.txt` discovers
them with a configure-time glob and content check rather than a hand-written
list. Composition-only examples run on every platform; an example containing
a runtime function or an `impl fn` takes the Unix-only scripted native path.
The generated fixtures compile every example through `hgl_add_module()` under
the repository warning policy, and the `../tests/codegen/generated_*_tests.cpp`
cases drive the generated graphs with hgraph's own harness. Where an example
has no `test` block, the note below says which unit tests carry its behaviour.

- [`midpoint.hgl`](midpoint.hgl) uses an internal helper, `export fn`, an
  atomic tuple, a `const` window, and a `test` of the unexported helper. It
  imports `hgraph.analytics`. Tests: 1 `test` under `hgl test`
  (`hgraph_language_test_midpoint`, gated on `hgraph::analytics`) and
  `generated_example_tests.cpp`.
- [`runtime-choice.hgl`](runtime-choice.hgl) contrasts a wiring-time topology
  choice, explicit time-series selection, and a `when` runtime function. It
  imports `hgraph.analytics`. Tests: none in the file;
  `generated_example_tests.cpp` builds it.
- [`stateful-node.hgl`](stateful-node.hgl) demonstrates aggregate state,
  grouped injectables, lifecycle blocks, ordered handlers, previous output,
  and incremental collection output. Tests: none in the file; native behaviour
  in `generated_example_tests.cpp`, and the `--dump-hir` /
  `--dump-hgraph-ir` CTest cases read this example.
- [`collection-views.hgl`](collection-views.hgl) demonstrates dual-phase
  `key_set`, runtime `keys`/`values`/`items`, built-in and inline predicates,
  `last_modified`, and mutable lexical `var`. Tests: none in the file; native
  behaviour in `generated_example_tests.cpp`.
- [`native-functions.hgl`](native-functions.hgl) defines real top-level C++
  scalar and collection-view helpers with `native fn`, declares their public
  hgraph view headers with `cpp include`, overloads `len` across list, set, and
  map HGL types, and calls the selected plain C++ function from runtime nodes.
  Tests: generated C++ formatting and descriptor import plus scalar/list/set/map
  ticks in `generated_inline_native_tests.cpp`.
- [`operators-and-generics.hgl`](operators-and-generics.hgl) demonstrates a
  nominal bodyless `operator`, a generic `impl fn` implementation, const-generic
  rolling-window sizes, an exported exact function, the default minimum window
  size, and a duration window. Operators, functions, and structs may carry
  the agreed `requires` constraint syntax. Tests: none in the file; generic
  resolution and window ticks in `generated_generic_tests.cpp` and, on the
  direct backend, `../tests/wiring/backend_coverage_tests.cpp`.
- [`structural-types.hgl`](structural-types.hgl) demonstrates a recursively
  temporal struct, `atomic<S>`, a type-generic struct, closed-set requirements,
  abstract-only inheritance with a default override, and a sparse delta in a
  runtime function, alongside temporal maps and an anonymous `fn`. Tests: none
  in the file; `generated_structural_tests.cpp`, and direct-wiring struct
  cases in `../tests/wiring/backend_tests.cpp`.
- [`reference-routing.hgl`](reference-routing.hgl) demonstrates `ref<T>`
  parameters and results in runtime functions: forwarding a reference, and
  routing one element of a `list<ref<T>, 3>` by a temporal index. Tests: none
  in the file; routing ticks in `generated_reference_tests.cpp` and, for the
  composition shapes, `../tests/wiring/backend_coverage_tests.cpp`.
- [`fixed-list-iteration.hgl`](fixed-list-iteration.hgl) demonstrates a
  graph-phase `for` over a fixed temporal list with `values` and `items`,
  wiring one body per child connection. Tests: none in the file; per-child
  wiring in `generated_iteration_tests.cpp` and
  `../tests/wiring/backend_tests.cpp`.
- [`dynamic-collection-iteration.hgl`](dynamic-collection-iteration.hgl)
  demonstrates a graph-phase `for` over a temporal map and an unbounded list,
  one sink child graph per key or index, with shared captures passed whole.
  Tests: none in the file; child-map ownership in
  `generated_iteration_tests.cpp` and recorded child ticks in
  `../tests/wiring/backend_coverage_tests.cpp`.
- [`conditional-result.hgl`](conditional-result.hgl),
  [`conditional-results.hgl`](conditional-results.hgl), and
  [`conditional-mixed-results.hgl`](conditional-mixed-results.hgl) exercise
  temporal branch results, escaping assignments, structural result packing,
  and remapping in both compiler backends. Tests: none in the files; ticks in
  `generated_tests.cpp` and `../tests/wiring/backend_tests.cpp`.
- [`conditional-forwarding.hgl`](conditional-forwarding.hgl) preserves an
  initialized result through an implicit or explicit unassigned branch and
  exercises independent reference forwarding for structural result fields.
  Tests: none in the file; ticks in `generated_tests.cpp` and
  `../tests/wiring/backend_tests.cpp`.
- [`conditional-omitted-else.hgl`](conditional-omitted-else.hgl) shows that a
  consumed temporal conditional without `else` produces no tick while false,
  using a type-resolved `nothing` branch in both compiler backends. Tests:
  1 `test` under `hgl test` (`hgraph_language_test_conditional-omitted-else`)
  and `generated_tests.cpp`.
- [`conditional-early-return.hgl`](conditional-early-return.hgl) returns from
  one temporal branch and composes the rest of the body as the other branch's
  continuation, for top-level, nested, tail, assigned, and outputless forms.
  Tests: 8 `test` blocks under `hgl test`
  (`hgraph_language_test_conditional-early-return`) and `generated_tests.cpp`.
- [`conditional-sinks.hgl`](conditional-sinks.hgl) controls child-graph
  lifetime with an outputless temporal conditional through the native sink
  switch, keeps a second sink outside it, and discards a sink conditional
  inside a value-producing graph. Tests: none in the file; `generated_tests.cpp`
  and `../tests/wiring/backend_tests.cpp`.

As compiler slices land, each example should advance from parsing and typed IR
coverage through `hgl test` to generated C++ behavior and backend parity.
The backend-parity module that is built both ways lives in
`../tests/codegen/parity.hgl`; the expression-embedded temporal conditional
is pinned there. The acceptance sequence is defined in the
[Developer Guide](../docs/developer-guide/testing-and-compatibility.md#documentation-examples).
