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
Scripted `test` accepts the corresponding `--native-provider-header` and
`--native-provider` options. Its cache is bypassed until provider-header
transitive dependencies can be fingerprinted.

The 31 scalar substrate implementations live in `stdlib/cpp/native_scalar.h`.
`native/scalar_values_i64.hgl` is a shared declaration part used by both target
implementations. `emit-native-rust <file> --out <file>` generates a checked Rust
trait for concrete bool/i64/f64 value declarations. Rust overloads, generics,
fallible contracts and other scalar mappings are rejected until implemented.

Temporal declarations retain their source role but cannot use this scalar
provider ABI. The temporal provider ABI and explicit collection-borrow spelling
remain outstanding. Existing inline view helpers retain their legacy behaviour
while that migration is pending; they are not examples of the new typing rules.
