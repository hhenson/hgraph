---
name: hgraph-write-operators
description: Define or review hgraph operator contracts and overload families in C++ and Python. Use when adding a C++ operator marker or Python `@operator`, replacing input-type or wiring-policy branches with overload selection, implementing node or graph overloads, registering C++ or Python candidates, designing defaults/resolvers/requires predicates, diagnosing overload ranking or ambiguity, generating the Python operator surface, or testing operator dispatch and cross-language parity.
---

# HGraph Operator Authoring

Define a public function-like contract once, then let wiring select the most
specific registered implementation for the concrete argument schemas and
fixed scalar choices. Keep the contract pure and the implementations typed.

## Read the existing family first

1. Read the current repository's `AGENTS.md`. In the hgraph core checkout,
   also read `docs/source/developer_guide/operators.rst`; in a downstream
   project, use its extension conventions and the public operator
   documentation for the pinned hgraph version instead.
2. Inspect the nearest operator marker, implementation header, registration
   translation unit, C++ tests, and Python compatibility tests.
3. For a compute or sink implementation, also follow
   `../hgraph-compute-sink-node/SKILL.md`. For a graph overload, follow
   `../hgraph-write-graphs/SKILL.md`.
4. Inspect `include/hgraph/types/operator_dispatch.h` only when the task needs
   defaults, `requires_`, output resolution, variadic arguments, or dispatch
   diagnostics beyond the established neighbouring pattern.

## Define a pure function contract

Treat an operator as a function interface. It names one logical operation,
describes the general call shape, and documents behaviour shared by every
implementation. It is not executable. "Pure contract" means
implementation-free here; a sink operator may still contractually describe a
side effect.

- Give the marker only its name and abstract signature. Do not put `eval`,
  `start`, `stop`, `compose`, state, or implementation decisions on it.
- Document the semantic result, tick/validity expectations, errors, wiring-time
  policies, and argument meanings. Describe the supported family without
  promising one representation or algorithm unless that is part of the
  contract.
- Choose parameter names deliberately; implementations should preserve those
  names and their roles so named calls remain coherent.
- Use broad base type contracts in the marker. Use independent type variables
  when operands or output may differ; repeat a variable only when equality of
  those types is part of the public contract.
- Match the public Python operator name exactly, including any trailing
  underscore.

For example:

```cpp
/** Normalize values according to the wiring-time mode.
    @param ts Values to normalize.
    @param mode Supported normalization contract.
    @return Values in the overload-selected output shape. */
struct normalize : Operator<"normalize",
                            In<"ts", TsVar<"S">>,
                            Scalar<"mode", NormalizeMode>,
                            Out<TsVar<"O">>>
{
};
```

The marker signature is documentary. Candidate matching uses each
implementation's own signature, which is what permits a single operator to
cover concrete, generic, and heterogeneous types through node and graph
realizations. Output-producing and sink contracts remain distinct call shapes.

## Use overloads instead of type switches

When implementation code starts branching on an input type, schema kind,
scalar type, or fixed policy, move that choice into operator resolution. Wiring
knows these facts and should select the implementation once.

- Add an overload to the existing operator when the behaviour already has a
  contract.
- Introduce a new operator when no contract exists for the functionality; do
  not leave a standalone type-switching node as the public abstraction.
- Use a graph-level dynamic switch only when the selector is time-series data
  and the choice must legitimately change during execution.
- Keep representation-only dispatch inside an established erased ops-table;
  do not use erasure to conceal semantic implementation selection.

## Refine the contract in implementations

Make every candidate more precise than, or otherwise compatible with, the
general function contract:

- Preserve the contract's base argument order, names, and semantic roles.
- Refine `TsVar`/`ScalarVar` inputs to concrete schemas, constrained variables,
  aligned repeated variables, or structural shapes that the implementation
  actually supports.
- Refine the output independently when the result differs from the inputs.
- Use a compute or sink node overload for primitive runtime work. Keep its
  logic in `eval` and its lifecycle work in `start`/`stop`.
- Use a graph overload when the implementation composes existing operations
  at wiring time.
- Use a concrete scalar type to refine by policy type. Use a context-aware
  `requires_` predicate to refine by a wiring-time scalar value, and use
  `resolve_default_types` only to bind output variables that inputs cannot
  determine. Keep candidates mutually exclusive or deliberately ranked.

An implementation may add positional parameters or keyword arguments not
shown by the abstract signature because dispatch is driven by candidate call
shapes. Use that freedom sparingly: overload-specific parameters are difficult
to discover. Prefer a common contract parameter, a separate operator, or a
strongly typed policy enum. When an extra is justified, document it in the
operator's public documentation and exercise the named call in both C++ and
Python tests.

## Register C++ overloads explicitly

Follow the standard family layout:

1. Put the abstract marker and its documentation in
   `include/hgraph/lib/std/operators/<family>.h`.
2. Put concrete node/graph implementations under the corresponding `impl`
   boundary, normally
   `include/hgraph/lib/std/operators/impl/<family>_impl.h`.
3. Register node candidates in the family registration translation unit with:

   ```cpp
   register_overload<normalize, normalize_float>();
   register_overload<normalize, normalize_tsd>();
   ```

4. Register graph candidates with:

   ```cpp
   register_graph_overload<normalize, normalize_tsl_map>();
   ```

5. Add a new family registration function to the standard installer
   (`install_standard_operators()` inside `registration.cpp`) when
   introducing a standard family. An extension exposes its own explicit
   registration entry point that records a KEYED INSTALLER and runs the
   installer list (RFC 0025 checkpoint 3):

   ```cpp
   OperatorRegistry::instance().register_installer("my.extension", installer);
   OperatorRegistry::instance().run_installers();
   ```

   The installer carries the extension's ENTIRE registration — native
   types, operator overloads, and (for python extensions) the
   `register_native_scalar_type` associations, capturing the `nb` class
   handles — because a registry reset clears all three. Resets keep
   installer intent, so one rebuild call replays the extension's
   registration exactly as core's — idempotent between resets; a
   throwing installer stays unapplied and is retried by the next
   rebuild.

When a new standard family is genuinely needed, also add its definition header
to `operators/operators.h`, its implementation header to
`impl/operators_impl.h`, and its registration translation unit to the CMake
source list. Prefer adding an operator to the nearest existing family over
creating a one-operator family.

Never register from a static initializer. Registries are reset between tests,
and candidate patterns borrow interned type metadata. Register standard types
first, then overloads. Tests that need standard operators call
`stdlib::register_standard_operators()` before wiring; it replays every
registered installer (core and extensions) not yet applied since the last
reset, so repeated calls are safe.

## Connect the same registry to Python

Keep C++ as the source of truth for core runtime semantics. The Python surface
must adapt to the same native operator registry rather than implement a second
dispatcher or a parallel runtime operation.

For a public native operator:

- Register the C++ overloads under the exact public name. The Python module
  calls `register_standard_operators()` at initialization, and registered names
  become lazy `hgraph.<name>` callables through `operator_function`.
- Preserve enough marker documentation and overload metadata for generated
  signatures and docstrings.
- Regenerate the public catalogue, Python typing declarations, runtime
  docstrings, and API inventory with:

  ```sh
  .venv/bin/python tools/api_inventory.py
  ```

- Add Python tests through the public operator name to prove authoring and
  bridge parity with the native C++ test.

For a Python-authored operator or extension overload, declare the contract and
attach implementations with the normal decorators:

```python
@operator
def normalize(ts: TIME_SERIES_TYPE, mode: NormalizeMode) -> OUT: ...

@compute_node(overloads=normalize)
def normalize_float(ts: TS[float], mode: NormalizeMode) -> TS[float]:
    ...

@graph(overloads=normalize)
def normalize_tsd(ts: TSD[K, TS[float]], mode: NormalizeMode) -> TSD[K, TS[float]]:
    ...
```

`overloads=` registers each Python candidate through
`register_python_overload`; the native matcher still owns argument
normalisation, type binding, ranking, `requires`, and selection. Use decorator
`resolvers=` and `requires=` for wiring-time facts. Do not write Python-side
type dispatch. When the behaviour belongs in core, provide the first-class C++
path and equivalent C++ tests rather than leaving it as a Python-only runtime
implementation.

## Test the contract and selection

Test through the operator, not by wiring the concrete candidate directly.

1. Register the intended overload family after test registry reset.
2. Use native `eval_node<Op>` or a minimal concrete graph to prove every
   Python-visible behaviour at the same level.
3. Cover the generic fallback and each more-specific winner. Include mixed
   types, structural shapes, scalar-value policies, defaults, invalid inputs,
   and output-only resolution as applicable.
4. Add no-match and ambiguity coverage when adding ranking-sensitive
   candidates. An accidental tie is a design error, not a registration-order
   selection rule.
5. Add equivalent Python `eval_node` coverage through the public operator.
6. Regenerate and check the API inventory for a public native operator.
7. Run the focused tests, then the acceptance gates required by `AGENTS.md`.

Before handing off, confirm that the marker contains no implementation, no
node chooses behaviour by inspecting an input type, every overload is
explicitly registered, overload-specific parameters are justified and
documented, Python uses the same registry name, and tests prove which candidate
wins rather than only the candidate's isolated arithmetic.
