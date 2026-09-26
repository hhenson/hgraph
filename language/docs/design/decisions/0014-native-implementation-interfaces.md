# ADR 0014: Native implementation interfaces

Status: accepted authoring model; migration in progress.

## Contract

An HGL native declaration defines the language contract. It contains no target
body, header, crate path or symbol map. C++ and Rust implement the same contract
in ordinary source files. Native-library builds own their dependencies.

`native` changes the implementation language, not the typing rules:

- `native fn` is temporal. Non-`const` parameters are time-series inputs and
  its result is temporal.
- A `const` parameter is fixed configuration. Even an all-`const` temporal
  function can produce ticks; it does not become a value function.
- `native const fn` is value-level. It has no independent activation or output
  ticks. Parameters and results follow ordinary `const fn` typing.
- A borrowed input must be explicit in the language contract. An ordinary
  value parameter cannot silently acquire endpoint access because it is native.

Use one checked function contract for HGL and native implementations. Function
and parameter `const`, overload selection, capability requirements and phase
checks have the same meaning. Binding consumes that contract; it does not
introduce a separate native type system or a third function kind.

For example, this contract admits a scalar implementation:

```hgl
module example.native

native const fn bit_and(lhs: i64, rhs: i64) -> i64
```

Removing `const` changes the contract to a temporal function. A scalar callable
must then fail binding validation. Neither parameter names nor scalar payload
types erase that distinction.

## Implementations

The compiler generates the implementation interface. A C++ library supplies a
class with static members and binds it through the generated `bind<T>()`:

```cpp
#include <native.h> // Generated from native.hgl.

struct Implementation {
    static hgraph::Int bit_and(hgraph::Int lhs, hgraph::Int rhs) noexcept {
        return lhs & rhs;
    }
};

inline constexpr auto native = example::native::native_interface::bind<Implementation>();
```

The corresponding Rust library implements the generated trait:

```rust
use example_native_interface::Native;

pub struct Implementation;

impl Native for Implementation {
    fn bit_and(lhs: i64, rhs: i64) -> i64 {
        lhs & rhs
    }
}
```

Both may delegate to existing native functions. Generated checks include the
resolved parameter/result types, temporal role and exception policy. C++
implicit conversions cannot satisfy a mismatched signature. Overloads retain
their HGL identities; compiler-generated candidate ordinals are not public API.

The compiler retains the selected contract through checked IR. The native
library build selects its provider; calls use that static implementation. No per-tick name lookup or
registration is introduced. Lifetime, validity, phase and error rules remain
part of the HGL contract; a matching native signature alone does not prove them.

## Outputs and capabilities

Separate the shared signature from target implementation requirements. The
[implementation-part rules](../native-implementation-parts.md) define matching
and acceptance cases. A temporal `{}` is a graph; `{ when; }` is a node.

Shared interface:

```hgl
module example.native
native fn accumulate(value: i64) -> i64
native const fn describe(value: i64) -> str
```

Selected target part:

```hgl
module example.native part cpp_impl
native fn accumulate(value: i64) -> i64 {
    inject out, logger
    when;
}
native const fn describe(value: i64) -> str {
    inject logger
}
```

The compiler checks these contracts; temporal execution still needs its provider
ABI. Value helpers support logger/clock in C++ and logger in Rust traits.

An HGL implementation requests capabilities in its body. The checked contract
records capability requirements for both forms, including descriptor imports.
They add no caller-supplied argument, temporal input or activation dependency.
A call silently adds the callee's required injectables to the caller's list.
Compute this transitively, independent of declaration order, and deduplicate
explicit and inferred requests. Missing `inject` in the caller is not an error;
an unavailable capability or forbidden phase still is.

The shared declaration owns types and observable behaviour. The selected
implementation part owns injectable requests and lifecycle hooks. C++ may
request `out, logger` while Rust requests only `out`; each generated interface
uses its selected requirements. Calls silently inherit selected value-helper
requirements. A constructed node owns its own capabilities. Target selection
may expose an unavailable service, which is a checking error. It cannot relax
promised observable behaviour. Provider-private allocators and scratch storage
remain native implementation details.

| Request | Capability type | Ownership and access |
| --- | --- | --- |
| `out` | `Output<T>` from the temporal result `-> T` | Current node; evaluation writes |
| `logger` | `Logger` | Supplied context; logging only |
| `clock` | `EvaluationClock` | Current run; read-only, admitted runtime phases |
| `scheduler` | `Scheduler` | Current node; admitted runtime phases |

These are language capability types, not payloads to temporalize. Their public
type spelling and target wrappers remain to be implemented. `out` designates
the existing result, never an additional output. An outputless function cannot
request it. For nested collections, its shape is the complete result schema.
Existing validity, delta and write-order rules still apply.

`const fn` means non-temporal, not pure. It may request a logger when the call
context supplies one; otherwise the call is rejected. Injection alone must not
classify it as a node. It returns its value directly and cannot request its own
temporal `out`, scheduler or node state. Access to a caller's node capabilities
needs an explicit ownership contract; that extension is not settled here.
Clock access requires a runtime context and an admitted phase, not merely a
`const fn` declaration. No injection implicitly creates a run or node.

Capability access is borrowed for the call. It cannot escape in a result,
state or cache. Check requirements at each call, including transitive calls,
and preserve them across imports. Do not constant-fold or reorder effects
merely because the function is `const`.

Illustrative target signatures for the declarations above (not existing APIs):

```cpp
static void accumulate(const Input<Int>& value, Output<Int>& out, Logger& logger);
static String describe(Int value, Logger& logger);
```

```rust
fn accumulate(value: Input<'_, i64>, out: Output<'_, i64>, logger: Logger<'_>);
fn describe(value: i64, logger: Logger<'_>) -> String;
```

C++ `bind<Implementation>()` and the Rust trait check their target's generated
signatures. The examples show implementations requesting both capabilities;
neither requires all targets to use that same parameter list. The selected
implementation part supplies the target requirements.
The first implementation publishes through `out`; its native `void`/unit result
does not remove the HGL temporal result. The second returns a scalar string.
The adapter for temporal implementations returning a complete value, and the
full lifecycle ABI, remain separate implementation work.

### Calling from a node

```hgl
fn describe_each(value: i64) -> str {
    when {
        return describe(value)
    }
}
```

During evaluation, `describe` receives the current scalar value and borrows the
enclosing node's logger. It returns a string; the enclosing `return` publishes
it. No helper node or output is created. The node omits `inject logger`: the
compiler silently adds the selected helper implementation's requirements.

A call to `native fn`, like a call to HGL `fn`, composes or wires a temporal
computation during graph construction. It is not a direct call inside `when`.
A helper that mutates the caller's output needs explicit borrowed access;
the spelling and checking of that access remain unsettled. Its scalar result
never implies ownership of the caller's temporal output.

### Acceptance cases

Acceptance expectations. Value-helper inference, imports, logger forwarding,
clock reads and ordinary/native call checks have executable coverage. Temporal
provider output and borrowed-output cases remain pending. Target-specific
requests, graph/node shape and lifecycle metadata have compiler coverage.

| Case | Expected result |
| --- | --- |
| `const fn` and `native const fn` with `i64` argument/result | Scalar types in both; no temporal argument introduced by binding |
| `fn` and `native fn` with `i64` argument/result | Temporal types in both; a parameter marked `const` remains configuration |
| Temporal `-> i64` with `inject out` | One `Output<i64>`; no extra output or activation dependency |
| Outputless or value function requests `out` | Diagnostic; a scalar return is not a temporal output |
| Value helper requests logger in a supplied logging context | Direct value call plus logging; no extra node or tick |
| Caller omits `inject logger`, including through a helper/import | Silently infer one logger requirement on each caller |
| Inferred capability has no valid provider in the call context | Diagnostic before emission |
| C++ uses `out, logger`; Rust uses only `out` | Same signature; each selected implementation part supplies its requirements |
| Provider requests a capability absent from its selected HGL implementation part | Binding diagnostic; internal runtime machinery needs no HGL declaration |
| Target cannot supply a required capability | Binding diagnostic; no silent fallback |
| Scheduler request without a node, or clock without runtime context | Diagnostic; no implicit owner |
| Capability used in a forbidden phase or retained after the call | Diagnostic |
| Node returns an identity logging helper's result for input ticks `2, _, 2` | Two helper calls and output ticks `2, _, 2`; no tick on the idle cycle |
| `describe_each` receives input ticks `2, _, 2` | Two direct helper calls using the same node's logger; two string ticks, no tick on the idle cycle |
| Temporal HGL/native function called inside `when` | Same diagnostic; no graph wiring during evaluation |
| Nested output receives two different child writes in one evaluation | One accumulated delta under the existing output rules |

Language diagnostics have no Python/C++ runtime oracle. For runtime cases,
record values, ticks, effects and owner identity against reasoned, Python and
C++ results before implementing bindings. Accept two-way agreement with a
variation report; if Python and C++ agree against reasoning, revisit reasoning.
Escalate three-way disagreement. The [helper reference traces](https://github.com/hhenson/hgl/blob/codex/native-interface-bindings/docs/compiler/capabilities/README.md)
agree across reasoning, Python and C++ for duplicate/idle ticks, helper call
counts and evaluation-clock reads. Compiler diagnostics are validated separately.

## Dependencies

HGL consumers import the shared interface. A package selects its implementation
once. Native imports stay in C++/Rust source; dependencies stay in that library's
ordinary CMake/Cargo build. The build exports generated binding information for
consumers, including the provider entry point and required link dependencies.
There is no separately authored per-function configuration or symbol manifest.

Generated descriptors are compiler/build output, not a second authored copy of
the interface. Checking an interface must not load its native library.

## Acceptance

| Scenario | Required result |
|---|---|
| Value `bit_and(6, 3)` | `2` in C++ and Rust |
| Temporal `bit_and` bound to scalar arguments/result | Compile error |
| All-`const` parameters on temporal function | Retain temporal result/activation |
| Wrong argument or result type | Native compile error, including convertible types |
| Missing member or incompatible overload | Native compile error |
| No-throw contract with potentially throwing C++ implementation | Compile error |
| Value helper called from a node | Existing activation and output tick rules |
| Borrowed input retained beyond its call | Rejected by lifetime contract |
| Provider missing or ambiguous | Diagnostic before executable generation |
| Nested REF introduced by substitution | Normalize before binding checks |

Record reasoned results before execution. Preserve existing scalar operator
traces against Python and C++; this migration must not change their ticks.
Collection-view spelling and its migration must be settled before replacing
existing view helpers. Opaque state and recovery require their own contracts.

## Migration status

Concrete scalar `native const fn` declarations compile through source-owned
C++ providers, including overload and exception checks. `hgl_add_module` takes
`NATIVE_PROVIDER_HEADER` and `NATIVE_PROVIDER` once per library; the C++ header
owns `bind<T>()`, member implementations and its includes. These are library
entry points, not per-function mappings. Installed sources include the provider.
The generated public C++ scalar wrappers retain their existing const-reference
ABI; provider methods use values for bool/i64/f64 and const references for other
scalars. `const` in that C++ spelling does not change HGL temporal roles.
Native value calls lift over time-series arguments through the same runtime
node policy as ordinary `const fn`, including imported calls. Descriptor format
8 records `value`, `temporal`, or `legacy-value` plus required capabilities;
hooks do not determine role. Capabilities propagate transitively through value
calls and imported native declarations, without duplicate injection requests.
`legacy-value` is a migration detail, not an agreed language function kind;
the separate native checking paths must converge on the common contract above.
Legacy inline `native fn` is rejected inside `const fn`: value helpers must
state `native const fn`. Calendar/duration helpers now use the scalar provider.
Scripted `test` accepts the corresponding `--native-provider-header` and
`--native-provider` options. Its cache is bypassed until provider-header
transitive dependencies can be fingerprinted.

The 56 scalar substrate implementations live in `stdlib/cpp/native_scalar.h`.
`native/scalar_values_i64.hgl` is a shared declaration part used by both target
implementations. `emit-native-rust <file> --part <implementation> --out <file>` generates a checked Rust
trait for concrete bool/i64/f64 value declarations, including a call-borrowed
logger. C++ value providers also admit the evaluation clock. Target-specific requests come from the selected implementation part. Rust overloads, generics, clock injection,
fallible contracts and other scalar mappings are rejected until implemented.

New temporal declarations retain their source role but cannot use this scalar
provider ABI. The temporal provider ABI and explicit collection-borrow spelling
remain outstanding. Existing inline view helpers retain their legacy behaviour
while that migration is pending; they are not examples of the new typing rules.
