# Native interface

Status: accepted boundary; descriptor validation, native declaration metadata,
canonical fingerprints, the lifecycle ABI, explicit descriptor authoring, and
exact canonical-value and overloaded collection-input-view evaluation calls in
AOT modules implemented;
normalized-wrapper generation, opaque state, and external scripted dependency
loading remain

## Purpose

HGL is intentionally not a general-purpose language. Native C and C++ libraries
are nevertheless necessary for efficient algorithms, stateful resources, and
capabilities that cannot be implemented as graph composition. This record
defines how those libraries can participate without adding arbitrary C++
syntax, header semantics, or ownership conventions to HGL.

This interface extends the package and lifecycle model in
[Modules and native extensions](modules.md). It is not a second module system.

## No nested native source

Ordinary HGL files do not contain `#include`, `extern`, raw C++, preprocessor
directives, or native statement blocks. In particular, native source cannot be
nested inside a composition or runtime function.

An inline escape would be ambiguous about whether it runs while wiring or on a
tick, which values are current and valid, whether it may block or throw, how its
references survive an evaluation, and whether state participates in
record/replay. Those questions are semantic and must be answered in an
importable contract, not inferred from arbitrary source text.

If mixed source is ever reconsidered, it requires a separate language decision.
It is not an implementation shortcut for the first native interface.

## Descriptor is the contract

A supporting native package publishes a versioned descriptor alongside its
headers and libraries. `hgl check` reads the descriptor without loading native
code. Generated builds and scripted execution use its build metadata to link or
load the matching provider and verify the same fingerprint.

For every exposed native declaration the descriptor records:

- canonical HGL module and declaration identity;
- declaration category: hgraph operator, exact native value function, native
  constructor, or lifecycle operation;
- complete HGL parameter and result types, including generic collection-view
  patterns used only for overload selection;
- permitted phases: wiring, start, evaluation, or stop;
- observable effects, including mutation, I/O, blocking, and allocation where
  relevant;
- value, owned, shared, or borrowed ownership and any dependent lifetime;
- whether a parameter receives its current scalar value or its live typed input
  view;
- exception and thread-safety policy;
- canonical C++ symbol or generated wrapper identity;
- required public headers, CMake packages, imported targets, and runtime image;
- module lifecycle entry points, provider identity, compatibility versions, and
  descriptor fingerprint.

The serialized representation is the canonical, versioned JSON selected in
[ADR 0004](decisions/0004-json-module-descriptors.md). The current compiler
emits its envelope, public/provider inventories, structured HGL signatures,
struct layouts, defaults, canonical types and constraints, and generated build
metadata. `hgl check` reads and validates one such descriptor without loading a
library or consulting a registry. Native declarations now encode exact C++
symbols, permitted phases, effects, parameter/result ownership and dependent
lifetimes, exception policy, thread-safety policy, opaque or atomic native type
associations, runtime images, and lifecycle ABI metadata. The reader enforces
the initial non-blocking/noexcept evaluation envelope, explicit mutable state,
borrow rules, and lifecycle consistency. The compiler can build an explicit
module catalog from one or more descriptors, resolve a selective or aliased
`use`, select an exact overload from canonical scalar or collection types,
carry that candidate through HIR and HGraph IR, and emit its reviewed
`cpp_symbol` as a direct call. The AOT CMake helper obtains
descriptors from directly linked targets. Locked transitive dependency closure
and external-package resolution for the scripted loader remain to be added. No
new HGL declaration syntax is implied by this list.

## Native declaration categories

### Hgraph operators

A temporal callable is a normal registered hgraph operator. The descriptor
exposes its nominal operator contract and provider candidates, and HGL resolves
it through the shared hgraph resolver. This is the path for graphs, nodes,
sources, sinks, adaptors, and services that accept temporal arguments.

An ordinary C++ scalar function is never lifted implicitly into one node per
call. A package that wants temporal use supplies and registers the corresponding
hgraph operator implementation explicitly.

### Exact native value and view functions

An exact native function is callable only in phases allowed by its descriptor.
A value parameter receives the current canonical scalar payload. A collection
`input-view` parameter receives the corresponding live `TSL`, `TSS`, `TSD`, or
rolling input view. This permits constant-time metadata operations such as
`len(value)` and the existing collection iterators without materializing a
collection or inspecting its schema on every tick. Construction or cleanup of
private native state is the next stateful slice.

Native declarations may share one canonical identity when their HGL signatures
differ. The compiler treats them as one overload family, unifies generic
collection patterns against the argument types, and records the unique selected
candidate before HGraph IR lowering. No implicit conversions or native-overload
ranking are involved: no match is an error, and overlapping matches are
ambiguous. A package should therefore publish disjoint patterns.

The generated C++ calls the declared symbol or its package-provided wrapper
directly. The direct-wiring backend does not emulate it: a runtime-bearing
program follows the existing generated, compiled, and loaded image path.

Calls from wiring-time constant evaluation, automatic temporal lifting, and
general compile-time execution are outside the first interface. Although the
phase metadata can describe wiring, start, evaluation, and stop, the compiler
accepts a call only in a phase named by the descriptor and the implemented
canonical-value and collection-view slices are exercised in evaluation.

### Opaque native state

An opaque native type exposes no fields, inheritance, pointer operations, or
layout to HGL. It may be passed only to native functions that name the same
descriptor identity.

The first state bridge is an owned RAII value. Construction occurs during
replay-aware node startup; destruction occurs with the aggregate state after
the stop phase. A resource that needs observable shutdown exposes a permitted
stop-phase operation in addition to its destructor.

Borrowed values are confined to the call or evaluation that produced them.
They cannot be returned, stored in state or output, captured, placed in a
collection, or embedded in an HGL struct. Shared and independently owned
reference forms require explicit retain/release or move/destruction contracts
and arrive after the RAII slice.

### Atomic native values

A native value that crosses a temporal port is not merely an opaque C++ type.
It must have a registered hgraph scalar identity and the public value, storage,
equality, hashing, conversion, and serialization operations required by every
context in which the descriptor permits it. HGL then exposes it as a nominal
atomic value through that canonical metadata.

The compiler does not infer those operations from a C++ class definition.

## Initial safety envelope

The first native-value implementation is intentionally narrow:

- value arguments and results are canonical scalar values or an opaque state
  value declared by the same module;
- collection arguments may use generic `list`, `set`, `map`, or `rolling`
  patterns only when the parameter explicitly requests `input-view` access;
- collection type and extent generics participate in compile-time selection but
  are not automatically exposed as runtime values;
- opaque state uses owned RAII storage and cannot cross a temporal port;
- evaluation functions are non-blocking and `noexcept`;
- mutation is restricted to an explicitly identified state argument;
- raw pointers, references, pointer arithmetic, callbacks, variadic calls, and
  open C++ templates are not representable;
- an open C++ template is still exposed only through an explicit specialization
  or wrapper; generic HGL input-view patterns erase to reviewed non-template C++
  view types;
- native declarations do not participate in implicit conversions;
- descriptor and loaded-provider fingerprints must agree before wiring.

These restrictions can be relaxed individually when a real core or extension
migration requires them and their semantics are defined. They must not be
relaxed by accepting arbitrary C++ text.

## Desired HGL experience

Once a native package descriptor exists, existing HGL import and runtime syntax
is sufficient at the call site for a canonical scalar helper. For example, a
package may expose a non-throwing scalar update function so an HGL node can be
written as:

```hgl
use acme.stats as stats

fn smooth(value: f64, const window: i64) -> f64 {
    state previous: f64 = 0.0

    when modified(value) && valid(value) {
        previous = stats::update(previous, value, window)
        return previous
    }
}
```

An overload family uses the same source syntax. Here `len` is a direct native
view operation, while a public temporal `len_` operator can call it from its
runtime implementation:

```hgl
use hgraph.native as native

impl fn len_<T, const size: i64>(value: list<T, size>) -> i64 {
    when modified(value) && valid(value) {
        return native::len(value)
    }
}
```

`T` and `size` select the list-view overload. The body does not need either
value: the selected C++ overload reads `value.size()` from the live input view.
This is the important distinction between a generic required to instantiate or
select a callable, a marker retained only as part of a type, and runtime
information explicitly available through a native view.

This example introduces no new HGL syntax. The source spelling and inference
rules for an imported opaque state type are not settled, so this record does not
invent an example for them. The native implementation must stop at that design
question if existing nominal type syntax is insufficient.

## Producing descriptors

The installed `hgl::native_package` C++ API is the first producer. A small
build-time executable owned by the native package fills an
`hgl::native::Package` and calls `write_descriptor`. The authoring model can
name canonical scalars, nominal native types declared by that same package,
and generic collection input-view patterns. It sorts set-like inventories and
declarations, creates the shared descriptor schema records, seals the result,
and runs the same validator used by `hgl check` before writing anything.

For example, this describes a non-throwing scalar operation:

```cpp
#include <hgl/native_package.h>

int main()
{
    using namespace hgl::native;
    Package package{
        .module_identity = "acme.stats",
        .language_version = "0.1",
        .declarations = {
            Declaration{
                .identity = "acme.stats::update",
                .cpp_symbol = "acme::stats::update",
                .parameters = {
                    Parameter{.name = "previous",
                              .type = ValueType::canonical(ScalarType::F64)},
                    Parameter{.name = "value",
                              .type = ValueType::canonical(ScalarType::F64)},
                },
                .result_type = ValueType::canonical(ScalarType::F64),
                .phases = {Phase::Evaluation},
            },
        },
        .build = Build{
            .public_headers = {"acme/stats.h"},
            .cmake_packages = {"acme_stats"},
            .imported_targets = {"acme::stats"},
        },
    };
    write_descriptor(package, "acme-stats.hgl-module.json");
}
```

The named `cpp_symbol` must already be an exact directly callable public C++
symbol. Multiple descriptor declarations may name the same C++ overload family
and HGL identity when their HGL signatures differ. If a template, throwing
function, or ownership-heavy API needs normalization, the package supplies a
small reviewed wrapper and names that wrapper. Automatic wrapper emission is a
remaining Stage F slice; the authoring API does not parse headers or accept
arbitrary C++ declarations.

For example, an erased list-view overload is described as:

```cpp
const ValueType i64 = ValueType::canonical(ScalarType::I64);
const ValueType t = ValueType::type_parameter("T");

Declaration{
    .identity = "hgraph.native::len",
    .cpp_symbol = "hgraph::native::len",
    .generics = {
        GenericParameter{.name = "T"},
        GenericParameter{.name = "N", .is_const = true, .type = i64},
    },
    .parameters = {
        Parameter{
            .name = "value",
            .type = ValueType::list(t, "N"),
            .access = ParameterAccess::InputView,
        },
    },
    .result_type = i64,
    .phases = {Phase::Evaluation},
}
```

The named C++ overload accepts `const hgraph::TSLInputView &` (or the view by
value) and returns `hgraph::Int`. Parallel declarations for `set<T>` and
`map<K, V>` form the same HGL overload family.

For AOT compilation, place the descriptor path on the native dependency
target's `HGL_MODULE_DESCRIPTORS` property and link that target from the HGL
module. `hgl_add_module()` passes those descriptors to every HGL compilation
and links the target that supplies the public header and symbol:

```cmake
set_property(TARGET acme_stats PROPERTY
    HGL_MODULE_DESCRIPTORS "${acme_stats_descriptor}")

hgl_add_module(my_hgl_nodes STATIC
    HGL smooth.hgl
    LINK_LIBRARIES acme_stats)
```

This bootstrap follows direct CMake target edges only. It does not yet compute
the locked transitive descriptor closure or teach `hgl test`, `hgl run`, and
the REPL how to resolve arbitrary external CMake packages and runtime images.

An optional Clang-based binding generator may later derive the same artifact
from annotated public headers. Clang is then a descriptor-generation tool, not
part of HGL parsing or the definition of which arbitrary C++ constructs the
language accepts. The generated descriptor remains reviewable and versioned.

## Module lifecycle and ABI

The descriptor participates in the existing closed package universe. Its
provider initializes transactionally, installs all registrations through one
module-owned handle, and deinitializes in reverse dependency order. Graphs,
plans, native call targets, and metadata retain provider leases.

The installed, C-compatible `hgl/native_module_abi.h` defines version one of
the dynamic lifecycle boundary. A provider exports the fixed
`hgl_query_native_module_v1` symbol. The host requests ABI version one and
validates the returned immutable table before activation. The table contains
its byte size, canonical module identity, descriptor fingerprint, opaque
module-owned context, and `init`, `deinit`, and `is_active` callbacks. An ABI
error record is host-allocated and has a fixed capacity; callbacks return a
status code and must not let exceptions cross the boundary.

The module, rather than the host loader, owns the hgraph provider handle and
all registration state behind the opaque context. Initialization and
deinitialization are idempotent. The scripted compiler bootstrap implements
this contract and catches registration/removal failures through hgraph's common
exception-boundary helper. The host validates the ABI version, table size,
identity, required callbacks, and exact descriptor fingerprint before it
retains the image and invokes lifecycle callbacks.

Descriptors are sealed with `sha256:` followed by the lowercase digest of their
canonical version-one semantic model with the fingerprint field empty. Input
whitespace and object ordering therefore do not affect identity. Compatible
unknown version-one members remain outside that projection; a security-relevant
semantic addition requires a format-version increment. The generated bootstrap
embeds the fingerprint, and the loader rejects an image whose module identity or
fingerprint differs before invoking `init`.

Calls within generated code may still use direct C++ types and functions when
the descriptor permits them. Logical provider removal and native-image
unloading are distinct: the first implementation deinitializes registrations
but deliberately keeps loaded images resident for process lifetime.

## Acceptance

The first native package proves:

- descriptor-only `hgl check` without loading its library;
- one canonical scalar value function used inside a runtime node in an AOT
  module;
- one owned opaque state value constructed at startup, mutated during
  evaluation, and destroyed after stop;
- rejection of the same calls in an unpermitted phase;
- rejection of a borrowed value that escapes;
- generated C++ that is a direct, readable call through public headers;
- scripted and ahead-of-time execution with identical ticks once external
  dependency resolution is implemented;
- descriptor/provider fingerprint mismatch before graph wiring;
- failed activation rollback and provider removal without stale registrations;
- an installed-SDK consumer build, not only an in-tree test.
