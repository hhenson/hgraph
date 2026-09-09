# Native interface

Status: accepted boundary; descriptor validation, native declaration metadata,
canonical fingerprints, the lifecycle ABI, explicit descriptor authoring,
source-defined inline C++ value/view functions, exact canonical-value,
overloaded collection-input-view, and payload-erased input-view evaluation
calls in AOT modules implemented; opaque state and external scripted dependency
loading remain

## Purpose

HGL is intentionally not a general-purpose language. Native C++ is nevertheless
necessary for efficient algorithms, stateful resources, and capabilities that
cannot be implemented as graph composition. This record defines two deliberate
boundaries: a small top-level source form for exact C++ value/view functions,
and versioned descriptors for separately built libraries. C and other
implementation languages remain future descriptor providers; the implemented
source escape is C++ only.

This interface extends the package and lifecycle model in
[Modules and native extensions](modules.md). It is not a second module system.

## Source native C++ functions

An HGL module may define a top-level exact native function and name the headers
required by its C++ projection:

```hgl
cpp include <hgraph/types/time_series/ts_input/list_view.h>

native fn len<T, const size: i64>(value: list<T, size>) -> i64 {
    cpp(const hgraph::TSLInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}
```

The outer HGL signature is authoritative for name resolution, generic
selection, constraints, parameter access, result type, and the generated module
descriptor. A temporal collection parameter is passed as its live typed hgraph
input view; an ordinary scalar temporal parameter is passed as its current C++
value. The input-only `signal` marker instead passes
`const hgraph::TSInputView &`. It is a payload-erased endpoint pattern which
accepts atomic values, structs, collections, windows, references, and
payload-free signals without exposing their payload type to HGL.
The `cpp(...)` list states the exact C++ parameter declarations received by the
body. The compiler supplies the function name, C++ result type, and `noexcept`,
then emits a plain function in the generated module's `native` namespace.
Same-named HGL candidates use distinct generated symbols: the first keeps the
short name and later candidates use `__candidate_N`. This is necessary because
two distinct HGL patterns can intentionally project to the same erased C++
view type, such as fixed and unbounded lists.

The source form is deliberately top-level. It cannot occur inside a graph or
node body, so it cannot introduce new wiring. Its first implemented phase is
runtime-node evaluation: a call inside `when` is a direct C++ call on current
values or views. A native declaration is automatically public because a
downstream module must be able to import it, and declarations with the same
name form one HGL overload family. Source-native `requires` clauses are rejected
until the version-one descriptor catalog can reconstruct them; the compiler
must not publish a contract it cannot enforce on import. Parameter defaults are
rejected because descriptor format v1 has no native-default contract.

HGL balances the C++ parameter list and compound statement while respecting
C++ comments, quoted literals, escapes, and raw string literals. It does not
implement a second C++ parser. The native compiler validates the projected C++
declarations and body. The generated header and source are run through the same
embedded `clang-format` policy as all other emitted code, so the escape remains
readable in review.

`cpp include <header>` and `cpp include "header"` accept literal system and
project header names. The compiler retains their delimiter form, deduplicates
after the first occurrence, and emits them before native declarations in the
generated header. They are local to the defining source module and do not
propagate through HGL imports. Macro, computed, and conditional includes are
rejected; CMake supplies header search paths and linked targets.

There is currently no HGL spelling for a link dependency, effect, throwing
policy, state type, lifecycle phase, or ownership annotation. Separately built
libraries use descriptors for those concerns. A future source feature must
define those contracts before widening this form.

This decision is recorded in
[ADR 0005](decisions/0005-inline-cpp-native-functions.md).

The first shipped use of this form is
[`hgraph.native`](../../stdlib/hgl/hgraph/native.hgl). Its compiled
`hgl::core_native` target provides `len` and `is_empty` for strings and typed
collection views. It also provides payload-erased `valid`, `all_valid`,
`modified`, `last_modified`, and `value_equals` functions over every standard
time-series shape. The source, generated library, header, and descriptor are
installed together and exercised by an isolated SDK consumer.

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
  patterns used only for overload selection and the complete input-only
  `signal` pattern used for payload-erased endpoint calls;
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
`cpp_symbol` as a direct call. A source `native fn` produces the same descriptor
record using its generated exact symbol. The AOT CMake helper obtains
descriptors from directly linked targets. Locked transitive dependency closure
and external-package resolution for the scripted loader remain to be added.
Native `requires` clauses also remain blocked until the catalog can reconstruct
their constraint arena.

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
rolling input view. A complete `signal` parameter with `input-view` access
receives the common `TSInputView` base instead. This permits constant-time
metadata operations and erased current-value behavior without materializing a
collection or switching on its runtime kind. Construction or cleanup of private
native state is the next stateful slice.

The installed `hgraph.native` module exposes this common endpoint surface:

| Native function | Erased hgraph operation | HGL result |
| --- | --- | --- |
| `valid(value)` | `TSInputView::valid()` | `bool` |
| `all_valid(value)` | `TSInputView::all_valid()` | `bool` |
| `modified(value)` | `TSInputView::modified()` | `bool` |
| `last_modified(value)` | `TSInputView::last_modified_time()` | `datetime` |
| `value_equals(left, right)` | `ValueView::equals()` on both current values | `bool` |

The one declaration for each operation covers `TS<T>` for every canonical or
registered atomic value, nominal `TSB`, fixed and unbounded `TSL`, `TSS`,
`TSD`, tick- and duration-based `TSW`, `REF`, and `SIGNAL`. That coverage comes
from hgraph's existing `SIGNAL` input compatibility and common view contract;
the implementation does not enumerate or branch over those types.

The remaining common erased operations are deliberately not disguised as
finished APIs:

| Missing surface | Required language or ABI feature |
| --- | --- |
| current `value` and `delta_value` results | an erased HGL value plus borrowed/dependent result lifetime |
| `reference()` | a dependent reference result whose target schema is selected from the argument |
| `hash()` | an agreed unsigned hash carrier and the throwing/unhashable contract |
| `compare()` | an HGL ordering result which represents less, equal, greater, and unordered |
| `to_string()` / `format_string()` | allocation and exception/effect declarations for source-native functions |
| dynamic-storage metrics and schema/type inspection | public HGL metadata value types |
| erased output access and mutation | an output-view parameter mode with explicit mutation and lifetime rules |

Specialized collection iteration remains on typed views and HGL intrinsics; it
cannot be represented by an erased scalar result without iterator and borrowed
element contracts.

Native declarations may share one canonical identity when their HGL signatures
differ. The compiler treats them as one overload family, unifies generic
collection patterns against the argument types, and records the unique selected
candidate before HGraph IR lowering. No implicit conversions or native-overload
ranking are involved: no match is an error, and overlapping matches are
ambiguous. A package should therefore publish disjoint patterns.

The generated C++ calls the declared symbol, its package-provided wrapper, or a
source-native generated function directly. It never subclasses an operator to
represent an exact helper. The direct-wiring backend does not emulate native
C++: a runtime-bearing program follows the existing generated, compiled, and
loaded image path.

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

The first native-value interface is intentionally narrow at its HGL boundary:

- value arguments and results are canonical scalar values or an opaque state
  value declared by the same module;
- collection arguments may use generic `list`, `set`, `map`, or `rolling`
  patterns only when the parameter explicitly requests `input-view` access;
- the complete input-only `signal` pattern may request `input-view` access and
  receives a common `TSInputView`; it is rejected as a value parameter, nested
  type, const parameter, or result;
- collection type and extent generics participate in compile-time selection but
  are not automatically exposed as runtime values;
- opaque state uses owned RAII storage and cannot cross a temporal port;
- evaluation functions are non-blocking and `noexcept`;
- mutation is restricted to an explicitly identified state argument;
- raw pointers, lifetimes, callbacks, variadic calls, and open C++ templates are
  not representable in the HGL signature or descriptor; a local C++ body is
  still real C++ and remains the author's responsibility;
- an open C++ template is still exposed only through an explicit specialization
  or wrapper; generic HGL input-view patterns erase to reviewed non-template C++
  view types;
- native declarations do not participate in implicit conversions;
- descriptor and loaded-provider fingerprints must agree before wiring.

These restrictions can be relaxed individually when a real core or extension
migration requires them and their HGL-facing semantics are defined.

## Desired HGL experience

A small helper owned by an HGL module can be implemented directly. The HGL
signature remains the public contract and the C++ projection states the exact
native ABI used by the generated call:

```hgl
native fn increment(value: f64) -> f64 {
    cpp(hgraph::Float value) {
        return value + 1.0;
    }
}

fn incremented(value: f64) -> f64 {
    when modified(value) && valid(value) {
        return increment(value)
    }
}
```

For a separately built native package, an imported descriptor supplies the
same call-site contract. For example, a package may expose a non-throwing
scalar update function so an HGL node can be written as:

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

An imported overload family uses the same call syntax. Here `len` is a direct
native view operation, while a public temporal `len_` operator can call it from
its runtime implementation:

```hgl
use hgraph.native as native

impl fn len_<T, const size: i64>(value: list<T, size>) -> i64 {
    when {
        return native::len(value)
    }
}
```

`T` and `size` select the list-view overload. The body does not need either
value: the selected C++ overload reads `value.size()` from the live input view.
This is the important distinction between a generic required to instantiate or
select a callable, a marker retained only as part of a type, and runtime
information explicitly available through a native view.

Payload-erased behavior uses the same direct-call model without a generic
overload family:

```hgl
use hgraph.native as native

fn observe(value: signal) -> datetime {
    when {
        return native::last_modified(value)
    }
}
```

The generated node accepts any standard time-series shape, while the C++ helper
receives only `const hgraph::TSInputView &`. HGL still cannot inspect the
payload of `value`; the native declaration exposes one reviewed operation on
that erased endpoint.

The source spelling and inference rules for an imported opaque state type are
not settled, so this record does not invent an example for them. The native
implementation must stop at that design question if existing nominal type
syntax is insufficient.

## Producing descriptors

The installed `hgl::native_package` C++ API is the first producer. A small
build-time executable owned by the native package fills an
`hgl::native::Package` and calls `write_descriptor`. The authoring model can
name canonical scalars, nominal native types declared by that same package,
generic collection input-view patterns, and a complete payload-erased signal
input-view pattern. It sorts set-like inventories and
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

An erased endpoint declaration uses `ValueType::signal()` and must select
`ParameterAccess::InputView`:

```cpp
Declaration{
    .identity = "hgraph.native::valid",
    .cpp_symbol = "hgraph::native::valid",
    .parameters = {
        Parameter{
            .name = "value",
            .type = ValueType::signal(),
            .access = ParameterAccess::InputView,
        },
    },
    .result_type = ValueType::canonical(ScalarType::Bool),
    .phases = {Phase::Evaluation},
}
```

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

The native boundary proves:

- descriptor-only `hgl check` without loading its library;
- one canonical scalar value function used inside a runtime node in an AOT
  module;
- one owned opaque state value constructed at startup, mutated during
  evaluation, and destroyed after stop;
- rejection of the same calls in an unpermitted phase;
- rejection of a borrowed value that escapes;
- generated C++ that is a direct, readable call through public headers;
- a source `native fn` emitted as a formatted plain C++ function, exported in
  the module descriptor, imported by another HGL module, and executed through
  generated C++;
- scripted and ahead-of-time execution with identical ticks once external
  dependency resolution is implemented;
- descriptor/provider fingerprint mismatch before graph wiring;
- failed activation rollback and provider removal without stale registrations;
- an installed-SDK consumer build, not only an in-tree test.
