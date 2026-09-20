# Native modules and packages

This guide is for extension and build-tool authors. HGL imports, calls, and
command-line usage are described in [Modules and tools](../user-guide/modules-and-tools.md).
Generated names, native storage, descriptor records, registration, and loader
behavior belong here rather than in the language-facing guide.

## Writing a small native C++ helper

Use a top-level `native fn` when node logic needs a direct calculation over
current values or a live hgraph collection view:

```hgl
cpp include <hgraph/types/time_series/ts_input/list_view.h>

native fn len<T, const size: i64>(value: list<T, size>) -> i64 {
    cpp(const hgraph::TSLInputView &value) {
        return static_cast<hgraph::Int>(value.size());
    }
}

fn list_size<T, const size: i64>(value: list<T, size>) -> i64 {
    when modified(value) && valid(value) {
        return len(value)
    }
}
```

The outer signature is HGL: it controls type checking, generic overload
selection, and what another module can import. The `cpp(...)` parameter list
and body are real C++. HGL generates the function name and return type, marks
the function `noexcept`, formats it with `clang-format`, and emits a direct call.
It does not create or subclass an hgraph operator for the helper.

A body that may raise says so with `throws` after the signature:

```hgl
native fn checked_reciprocal(value: f64) -> f64 throws {
    cpp(hgraph::Float value) {
        if (value == 0.0) { throw std::domain_error("checked_reciprocal: division by zero"); }
        return 1.0 / value;
    }
}
```

The generated function is then not `noexcept`. A raise ends the node's
evaluation: if the node's error output is captured (`exception_time_series`,
or `try_except` around the graph) it ticks a `NodeError` carrying the message;
otherwise the exception propagates out of the graph. Outputs written earlier in
the same evaluation stand, so call fallible helpers before writing. HGL has no
`try` of its own. A `noexcept` body that raises anyway terminates the process,
as in C++.

For temporal `list`, `set`, `map`, and `rolling` parameters, C++ receives the
corresponding live input view. A scalar temporal parameter receives its current
value. Generics in the HGL signature can select the overload even when the C++
view erases those details. For example, `size` participates in matching
`list<T, size>` but need not be a C++ parameter merely to call `value.size()`.
The contextual `schema` parameter is the narrow runtime-metadata case: it
receives a borrowed `const hgraph::TSValueTypeMetaData *` obtained from
`schemas(pack)` and cannot be stored or returned.

Native declarations are automatically public and same-named declarations form
an overload family. Generated C++ keeps these as plain free functions and gives
each candidate a stable readable symbol (`len`, `len__candidate_2`, and so on),
so erased HGL distinctions such as fixed versus unbounded list shapes cannot
create a C++ redefinition. Source-native `requires` clauses currently fail closed
because descriptor constraints are not reconstructed by the current import
catalog. Native parameters cannot have defaults. This first form runs only
in `start`, `when`, and `stop` when its parameters are all values, and only
during evaluation when it takes a live collection view; it cannot be nested
inside another function.
Use `cpp include <header>` for a system header or `cpp include "header"` for a
project header needed by source-native signatures or bodies. These declarations
are local to this source module, retain their order and delimiter form, and are
deduplicated in the generated header. They do not follow HGL imports. Configure
header search paths and linked libraries on the `hgl_add_module()` CMake target;
macros and conditional includes are deliberately not HGL syntax.

There is no general source syntax yet for linked libraries, state, lifecycle,
ownership, or general effects; `throws` is the supported exception annotation, and the immutable lifetime of a
`schema` parameter is fixed by that type. Use a separately built
descriptor-backed native package for other cases. `hgl check` validates the
parsed HGL contract and the balanced C++ boundary. `emit-cpp` additionally
validates that the generated descriptor fits the version-one native ABI.
Native compilation validates the C++ declarations and body.

The complete, compiled example is
[`native-functions.hgl`](../../examples/native-functions.hgl).

## Compiled module lifecycle

Each dynamically loaded scripted module has compiler-generated lifecycle entry
points. Module initialization attaches the library to the application and
records a keyed installer containing its type and operator registrations.
Registry installation may run again after an hgraph registry reset without
repeating unrelated module initialization effects.

The generated application initializes every module in dependency order before
wiring a graph. Deinitialization proceeds in reverse dependency order and
removes the module from future function and operator resolution, removes its
installer so a reset cannot restore it, and releases its registrations and
resources.

A live graph may retain generated code or metadata from a provider module.
Such a graph holds a lease on that module: deinitialization must wait or fail
while the module is in use, and the native library cannot be unloaded until all
leases are released. The first implementation may remove registrations while
keeping the native image resident; physical unloading is a stricter later
capability.

These entry points are generated infrastructure, not HGL `init` or `deinit`
blocks. The installed `hgl/native_module_abi.h` contract uses one versioned
query function returning module-owned `init`, `deinit`, and `is_active`
callbacks plus identity and fingerprint metadata. Native C++ extensions may
attach resource hooks behind that opaque module context, but language source
cannot perform arbitrary module-load side effects. Logical deactivation does
not imply that the library image is unloaded.

The AOT `hgl emit-cpp` / `hgl_add_module()` path currently emits a descriptor
and an explicit `register_operators()` function, not the dynamic lifecycle query
ABI. Its linked application therefore owns registration lifetime until AOT
lifecycle bootstrap generation is implemented.

## Building a package

`hgl emit-cpp` turns a module into ordinary hgraph C++ plus its reviewable module
descriptor. `prices.hgl` with `module examples.prices` becomes `prices.h`,
`prices.cpp`, and `prices.hgl-module.json`:

```cpp
namespace examples::prices
{
    namespace operators
    {
        using smooth = hgraph::Operator<"examples.prices.smooth", ...>;
    }
    struct smooth
    {
        static constexpr auto name = "examples.prices.smooth";
        static auto defaults() { return std::tuple{hgraph::arg<"window">(hgraph::Int{20})}; }
        static hgraph::Port<hgraph::TS<hgraph::Float>> compose(
            hgraph::Wiring &, hgraph::Port<hgraph::TS<hgraph::Tuple<hgraph::Float, hgraph::Float>>>,
            hgraph::Scalar<"window", hgraph::Int>);
    };
    hgraph::OperatorProviderHandle register_operators();
}
```

Exported functions become graph structs a C++ author wires with
`wire<examples::prices::smooth>(w, tob, hgraph::Int{20})`, and — after
`register_operators()` — operators any hgraph front end reaches by name,
`examples.prices.smooth`. The returned provider handle owns that registration.
Module-internal functions stay inside the `.cpp`.

The compiler runs both generated C++ files through `clang-format` before it
prints, writes, caches, or compiles them. `clang-format` is therefore a tool
dependency of `hgl`; set `HGL_CLANG_FORMAT` to select a particular executable.
The repository's `.clang-format` policy is embedded in the compiler, so the
result does not depend on a consuming project's local formatter settings. The
generated `operators` namespace contains transparent type aliases rather than
derived marker classes, so the registry contract visible in the source is the
exact hgraph `Operator` type.

The JSON sidecar is canonical and versioned. It records the module and language
versions; public structures, operators, and functions; implementation
candidates and provider requirements; and the generated build boundary. Its
structured schema records preserve generic bindings, parameters and results,
struct inheritance and effective fields with each field's recursive-edge mark
(ADR 0012), defaults and rolling bounds, nominal type applications, and
`requires` constraints. Integer and float literal
payloads are tagged strings so the full i64 range and non-finite floats remain
valid JSON.

For example, a generic operator points to descriptor-local type records rather
than embedding source text that another tool would need to parse:

```json
{
  "category": "operator",
  "identity": "examples.windows.summarize",
  "signature": {
    "generic_parameters": [
      {
        "name": "T",
        "kind": "type",
        "binding": "examples.windows.summarize::T",
        "type": null
      }
    ],
    "parameters": [
      {
        "name": "window",
        "kind": "signal",
        "binding": "examples.windows.summarize::window",
        "type": 1,
        "default": null
      }
    ],
    "result": 2,
    "requires": null
  }
}
```

The `type`, `result`, `default`, and `requires` numbers refer to records in the
same file's `schema` object. They have no identity outside that one descriptor.

In descriptor format v6, a parameter's `"kind"` is `"const"` for fixed
configuration, `"signal"` for a temporal parameter (`window` above is a
`rolling<T, ...>`), and `"runtime"` for an evaluation-local native schema handle. That `signal` is a parameter-role label and is unrelated
to the `signal` type of [Types and expressions](../user-guide/types-and-expressions.md).
The current reader accepts format v6 only; historical format changes are
recorded in [ADR 0004](../design/decisions/0004-json-module-descriptors.md).

Validate a descriptor without loading its native library:

```sh
hgl check build/generated/prices.hgl-module.json
```

This checks the versioned envelope, required field types, record shapes, and
all descriptor-local schema references. Object order and whitespace do not
matter; duplicate keys and unsupported versions are errors, while unknown
members are ignored for forward-compatible additions. Syntax and IR dump flags
apply only to HGL source and are rejected for descriptors.

This command validates one descriptor. Native declarations describe where a
function may run, its effects, value ownership and borrowed lifetimes, exception
policy, and thread-safety policy. Validation rejects unsafe combinations such
as blocking evaluation code, implicit mutation, shared ownership
in ABI version 1, and borrowed results without a declared input lifetime. It
also verifies the descriptor's canonical SHA-256 fingerprint and lifecycle ABI
metadata without loading native code. A `translated` exception policy is
allowed during evaluation; a source `throws` declaration selects that policy
([ADR 0009](../design/decisions/0009-native-errors-and-the-node-error-model.md)).

Descriptor validation does not yet locate or lock transitive provider
requirements. For source compilation, each repeatable `--module-descriptor`
option adds one explicitly named module to the import catalog. The compiler can
currently lower exact canonical-value functions and overloaded collection-view
functions used during runtime evaluation. For example, `len(value)` can select
a native list, set, or map overload and read the live collection size.
Unsupported ownership, effects, nominal native types, or phases are diagnosed
at the import or call boundary rather than silently approximated.

Native libraries create descriptors with the installed C++ target
`hgl::native_package` and `<hgl/native_package.h>`. Its public model is narrower
than the descriptor format: a signature can contain canonical scalars, a
nominal native type declared by that package, or a generic `list`, `set`, `map`,
or `rolling` input-view pattern. `descriptor_json(package)`
returns canonical sealed JSON; `write_descriptor(package, path)` additionally
writes it for installation. Both reject the same unsafe phase, effect,
ownership, borrow, and lifecycle combinations as `hgl check`.

The package names either an exact public C++ function family or its own reviewed
normalizing wrapper in each declaration's `cpp_symbol`. Declarations sharing an
HGL identity form an overload family and must have distinguishable exact type
patterns. The authoring API does not parse C++ headers and does not make
arbitrary templates part of HGL. See [Native interface](../design/native-interface.md#producing-descriptors)
for the complete example and current wrapper boundary.

A package is a CMake project. `hgl_add_module()`, installed with `hgl` in
`lib/cmake/hgl/HglLanguage.cmake`, runs `emit-cpp` at build time and compiles
the result beside any hand-written C++:

```cmake
find_package(hgraph CONFIG REQUIRED)
include(${hgraph_DIR}/../hgl/HglLanguage.cmake)   # or list(APPEND CMAKE_MODULE_PATH ...)

hgl_add_module(prices
    HGL prices.hgl
    PARTS signals.hgl statistics.hgl
    SOURCES native_helpers.cpp
    LINK_LIBRARIES hgraph::analytics
    PYTHON_MODULE _prices)
```

With `PARTS`, `prices.hgl` is the anchor whose filename determines the three
generated artifact names. It and every listed part declare the same module
with `part <name>`, and the compiler emits one logical module. Without
`PARTS`, multiple files in `HGL` remain independent modules which happen to be
built into the same CMake library.

The library `prices` publishes its generated headers and exposes its descriptor
paths through the CMake target property `HGL_MODULE_DESCRIPTORS`. When a target
listed directly in `LINK_LIBRARIES` has the same property, `hgl_add_module()`
passes those descriptors to `hgl emit-cpp`, makes them build dependencies, and
links the target that supplies the native header and exact symbol. This initial
bootstrap follows direct target edges; it does not yet calculate a transitive
locked package closure. `PYTHON_MODULE` adds a
stable-ABI extension module whose import registers every operator the HGL
modules export (its bootstrap is generated at build time from each module's
descriptor, which carries the registration symbol the compiler spelled; the
same spelling is what `hgl emit-cpp <file> --print-namespace` prints), and a
Python package directory with one generated wrapper
module per source so that

```python
from prices import smooth      # operator_function("examples.prices.smooth")
```

works exactly as it does for `hgraph_analytics`. Placement is yours:
`OUT_DIR` puts header and source in one directory, `INCLUDE_DIR` / `SRC_DIR`
split them; the default is `${CMAKE_CURRENT_BINARY_DIR}/hgl/<target>/`.
For a `SHARED` package, `hgl_add_module()` exports generated symbols from the
Windows DLL so descriptor-selected source-native calls remain linkable from a
consumer module.
The native extension is placed directly beside the wrappers for single- and
multi-configuration generators. Replacing an installed `hgl` executable also
invalidates the generated files. HGL export names that are Python keywords use
a trailing underscore in this wrapper (`class` becomes `class_`) while their
operator registry name remains unchanged; aliases that would collide are a
generation error.

What `emit-cpp` lowers today includes every checked-in example: composition
functions, runtime functions and sinks, scheduler-driven sources, source operators and implementations,
nominal and generic structs, fixed and duration rolling windows, sparse struct
deltas, concise functions passed to `map`, collection inputs and iteration,
scalar recordable state or scalar cache fields, ordered `when` handlers,
`inject out`, keyed TSD output writes, `inject logger`, `inject clock`,
`inject scheduler`, lifecycle blocks over state and `const` configuration,
and exact canonical-value or collection-view calls imported from native
descriptors during runtime evaluation. Native calls remain direct and readable
in generated C++; the compiler does not synthesize an operator subclass or
implicit node. The generated package tests compile every example and execute a
native-call fixture as C++.

It reports unsupported forms before writing output: calls to other temporal
HGL functions from runtime evaluation, non-scalar or opaque state/cache, mixed
state/cache declarations, lifecycle access to temporal inputs or output,
optional-field clearing in a sparse delta, generic constructor inference, typed
`const` generic struct metadata, compound constant literals, runtime-node `if`
used as a value, and zoned or civil literals. See the
[status matrix](../design/roadmap.md#feature-status-matrix-2026-09-07) for the
remaining type and expression boundaries.

## One execution model

The target architecture gives `test`, `run`, `emit-cpp`, and the REPL one
checked semantic IR and one hgraph runtime. The direct evaluator now consumes
hgraph IR. C++ generation uses the same IR for module, callable, operator,
export, registration, type, signature, internal dependency planning, and
composition and runtime bodies, whether concise or block-shaped (including
concise functions passed to `map`). A program made only of composition
functions is wired onto the runtime directly, in process. A file-based `test`
or `run` containing supported runtime functions goes through generated C++, as
does an ahead-of-time package.
The REPL selects the same two routes from the accepted session:

```text
source -> typed HIR -> hgraph IR -> direct wiring -> hgraph runtime
                              \-> C++ backend -> native -> hgraph runtime
```

Both backends now consume hgraph IR, including composition and runtime bodies.
Shared admission planning lives in `src/hgraph_ir/plan.cpp`; backend-parity
tests compare observable ticks over the supported composition subset.
The scripted image resolves hgraph symbols
from the running `hgl` process, so it registers into that process's registry
rather than linking a second static runtime. The compiler's parity suite holds
the shared composition subset to the same ticks and executes the runtime
subset through both the scripted and ahead-of-time compiled paths.

An imported native function may add public headers, CMake packages, linked
targets, and runtime images that do not belong to the compiler process. AOT
modules receive that build context from `hgl_add_module()` today. Scripted
commands validate and lower explicitly supplied descriptors but do not yet
resolve arbitrary external package build metadata. Native calls that require
that context are therefore AOT-only for now; descriptor-only `check` remains
available to scripted workflows.

The native path caches complete images by a SHA-256 key over the emitted code,
the resolved compiler binary and its version/target and effective options,
build profile, hgraph identity, relevant compiler environment, and the hosting
`hgl` executable. The default root follows the platform per-user cache
convention; `HGL_CACHE_DIR` overrides it. If either executable cannot be
identified or no per-user cache root is available, the command uses a transient
image instead of a shared temporary cache.
`HGL_DISABLE_CACHE=1` forces a transient compile, while `HGL_CACHE_TRACE=1`
prints cache hits, misses, and publication fallbacks. `HGL_ARTIFACT_DIR`
selects where transient and failed builds are written, `HGL_CXX` overrides the
compiler, and `HGL_CLANG_FORMAT` overrides the formatter used for generated
C++. Cache entries are immutable and safe for concurrent command
processes; this prototype does not yet prune them automatically.

The initial REPL rebuilds the whole session after each accepted runtime
declaration.
That is slower than a JIT but guarantees that exploration sees the same
function classification, overload, graph, node, and scheduling semantics as an
ahead-of-time production binary.

When a REPL module changes, the driver first compiles and loads the complete
candidate image without activating it, then removes the old revision's provider
at the quiescent prompt boundary and activates the replacement. An activation
failure reactivates the old image; a frontend, emission, or native compile
failure never touches it. Native images remain mapped for process lifetime,
while removed candidates and installer intent cannot survive a registry reset.

External input is supplied by imported native facilities or purpose-built
testing sources. A REPL convenience must not become an interpreter-only push
adaptor.
