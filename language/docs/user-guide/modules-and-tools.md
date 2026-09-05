# Modules and tools

Modules make hgraph and extension capabilities available without exposing a
general native foreign-function interface.

## Declaring a module

Every source file begins with one module declaration:

```hgl
module examples.prices
```

The module name is canonical and dot-separated. Files that contribute to the
same module will be a package-level feature; the atomic frontend initially
accepts one source file per compilation unit.

## Importing declarations

`use` can import an explicit set of public declarations into the local scope:

```hgl
use hgraph.std::{if_then_else}
use hgraph.analytics::{rolling_mean, zscore}
```

Alternatively, a module alias creates a namespace without adding its
declarations as unqualified names:

```hgl
use my.analytics as analytics

analytics::zscore(prices)
```

An `operator` declaration is public automatically. An ordinary exact function
is public only when declared `export fn`, and a nominal struct only when
declared `export struct` or `export abstract struct`; unexported functions and
structs remain module-internal. Both selective and aliased imports expose only
this public declaration surface.

There are no wildcard imports or re-exports. In particular, an implementation
module does not create a second import path for an operator defined elsewhere.
Canonical scalar and container types, temporal-shape rules, `atomic<T>`, and
expression operator bindings come from the implicit hgraph prelude. Exported
nominal structs and their abstract-family relationships follow their defining
modules and are imported like other public declarations. Everything else must
be imported, reached through a module alias, or declared locally.

An exported generic struct exposes one nominal family, not a separate export
for every possible specialization. Downstream code applies that family with a
complete argument list such as `Box<f64>`; the application target records and
registers only the concrete specializations it actually uses.

## Operator identity and implementation binding

An operator is identified by its defining module and name, not by its short
name alone. The canonical identities `market.pricing::value` and
`risk.pricing::value` therefore denote distinct contracts. Canonical identities
appear in diagnostics and metadata; source calls qualify through a local module
alias rather than spelling a dotted module path as an expression.

An implementation module binds an `impl fn` to a local operator declaration
or to exactly one operator brought into local scope by a selective import:

```hgl
module market.pricing_impl

use market.pricing::{value}

impl fn value(input: f64) -> f64 =>
    input
```

The `impl` modifier is what creates the binding. Without it, `fn value` beside
an in-scope operator `value` is a name conflict; with it and no operator in
scope, the declaration is an error. Adding a `use` for an unrelated call can
therefore never turn one of your helpers into a published candidate.

The uniqueness rule applies per local short name. Selectively importing two
different operator definitions as `value` is an import error before function
checking. A module alias does not establish an implementation binding, so an
implementation module may still use other same-named operators explicitly:

```hgl
module market.pricing_impl

use market.pricing::{value}
use risk.pricing as risk

impl fn value(input: f64) -> f64 =>
    input

fn compare(input: f64) -> bool =>
    value(input) == risk::value(input)
```

At a call site, name resolution selects the nominal operator first. Hgraph's
resolver then considers only implementations registered for that operator.
Namespace qualification therefore resolves collisions between different
operator definitions; it does not break a tie between implementations of one
operator. Equal-ranked implementations within one selected operator remain an
ambiguity error.

Every `impl fn` contributes an implementation candidate. It does not use
`export` and is not separately importable by its implementation module's
name. `export fn` is reserved for exposing an ordinary exact function.

## Implementation discovery

Imports control source names; they do not decide which operator implementations
are installed. The complete candidate universe comes from the resolved
application target:

- every source module belonging to the target;
- every module contributed by a locked package dependency;
- the hgraph kernel and selected native extension packages.

The compiler reads each module descriptor and indexes its candidates by the
operator's canonical identity. A provider module participates even when none of
its exact functions is imported. Conversely, a package installed somewhere on
the machine contributes nothing unless it belongs to the target's dependency
closure. This makes overload resolution reproducible and avoids scanning the
environment for implementations.

For an operator such as `add`, source imports its one defining contract. User
and extension modules in the target may contribute many implementations, but
they do not re-export or rename `add`. If equally specific candidates overlap,
the build reports their provider modules as an ambiguity rather than using
module or registration order as a tie-break.

Each imported declaration and each provider in the target closure is checked
against a language module descriptor. A descriptor contains public exact
functions, nominal operator identities, implementation candidates with provider
provenance, versions, required public headers, CMake package and target names,
and lifecycle and registration entry points. It does not grant access to
arbitrary symbols in a library.

## Compiled module lifecycle

Each compiled module has compiler-generated lifecycle entry points. Module
initialization attaches the library to the application and records a keyed
installer containing its type and operator registrations. Registry installation
may run again after an hgraph registry reset without repeating unrelated module
initialization effects.

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
blocks. Native C++ extensions may attach resource hooks through the module ABI,
but language source cannot perform arbitrary module-load side effects.

## Native adaptors stay native

A C++ extension may expose a typed function contract backed by a graph, node,
source, sink, service, or operator overload:

```hgl
use acme.market_data::{subscribe_quotes}
```

The language call can supply temporal arguments and `const` configuration
declared by that module. The C++ package remains responsible for callback
admission, threads, queues, backpressure, resource ownership, start and stop,
protocol acknowledgement, and teardown.

Language source cannot declare an adaptor or embed C++.

## Command-line workflow

The intended command surface is:

```text
hgl check path/to/program.hgl [--dump-tokens] [--dump-ast] [--dump-hir]
hgl test path/to/program.hgl [test-name]...
hgl run path/to/program.hgl [--entry name] [--mode sim|realtime]
        [--start <datetime>] [--end <datetime|duration>]
        [--set name=<constant expression>]... [--config run.toml]
hgl emit-cpp path/to/program.hgl [--out-dir <dir> | --include-dir <dir> --src-dir <dir>]
        [--python <file.py> --python-native <module>] [--print]
hgl repl
```

| Command | Behavior |
| --- | --- |
| `check` | Parse and resolve without compiling; the current prototype also constructs and can dump its resolved HIR |
| `test` | Run the module's `test` declarations and report failing assertions |
| `run` | Bind an entry to a mode, clock, and parameters, then execute it |
| `emit-cpp` | Write the module as `program.h` and `program.cpp`, public hgraph C++ in the module's namespace |
| `repl` | Accumulate declarations, run tests and `eval` forms interactively |

[Testing and running](testing-and-running.md) shows `test`, `run`, and the
run configuration file from the author's side.

The current `hgl` implements `--help`, `--version`, `check`, `test`, `run`
(without `--config`), `emit-cpp`, and `repl` over the `hgraph.std` and
`hgraph.analytics` kernels. File-based `test` and `run` compile/load the
supported scalar runtime-node subset through a native cache on Unix; the REPL
uses the same route when its session contains runtime declarations. `test`
accepts test names after the file to run a selection.
`check --dump-hir` is a compiler-development view with stable IDs and source
ranges. Its leading `HIR resolved` state is intentional: complete type, phase,
and effect checking is the next compiler stage, so the dump is not yet a
promise that every expression is typed.
The first-pass limits are listed in
[Testing and running](testing-and-running.md#first-pass-limits); the
constructs `emit-cpp` does not yet lower are listed under
[Building a package](#building-a-package).

## Building a package

`hgl emit-cpp` turns a module into ordinary hgraph C++. `prices.hgl` with
`module examples.prices` becomes `prices.h` and `prices.cpp`:

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

A package is a CMake project. `hgl_add_module()`, installed with `hgl` in
`lib/cmake/hgl/HglLanguage.cmake`, runs `emit-cpp` at build time and compiles
the result beside any hand-written C++:

```cmake
find_package(hgraph CONFIG REQUIRED)
include(${hgraph_DIR}/../hgl/HglLanguage.cmake)   # or list(APPEND CMAKE_MODULE_PATH ...)

hgl_add_module(prices
    HGL prices.hgl signals.hgl
    SOURCES native_helpers.cpp
    LINK_LIBRARIES hgraph::analytics
    PYTHON_MODULE _prices)
```

The library `prices` publishes its generated headers; `PYTHON_MODULE` adds a
stable-ABI extension module whose import registers every operator the HGL
modules export, and a Python package directory with one generated wrapper
module per source so that

```python
from prices import smooth      # operator_function("examples.prices.smooth")
```

works exactly as it does for `hgraph_analytics`. Placement is yours:
`OUT_DIR` puts header and source in one directory, `INCLUDE_DIR` / `SRC_DIR`
split them; the default is `${CMAKE_CURRENT_BINARY_DIR}/hgl/<target>/`.
The native extension is placed directly beside the wrappers for single- and
multi-configuration generators. Replacing an installed `hgl` executable also
invalidates the generated files. HGL export names that are Python keywords use
a trailing underscore in this wrapper (`class` becomes `class_`) while their
operator registry name remains unchanged; aliases that would collide are a
generation error.

What `emit-cpp` lowers today includes every checked-in example: composition
functions, runtime functions and sinks, source operators and implementations,
nominal and generic structs, fixed and duration rolling windows, sparse struct
deltas, concise functions passed to `map`, collection inputs and iteration,
scalar recordable state, ordered `when` handlers, `inject out`, keyed TSD output
writes, `inject logger`, and lifecycle blocks over state and `const`
configuration. The generated package tests compile every example as C++.

It still reports, by name, and writes nothing for generated runtime sources,
runtime function calls, non-scalar state, injectables other than `out` and
`logger`, lifecycle access to temporal inputs or output, optional-field clearing
in a sparse delta, generic constructor inference and typed `const` generic
struct metadata, compound constant literals, `if` used as a value, and zoned or
civil literals.

## One execution model

The target architecture gives `test`, `run`, `emit-cpp`, and the REPL one
checked semantic IR and one hgraph runtime. The direct evaluator now consumes
hgraph IR. C++ generation uses the same IR for module, callable, operator,
export, and registration planning, while a temporary source adapter still
prints function bodies, types, and signatures. A program made only of
composition functions is wired onto the runtime directly, in process. A
file-based `test` or `run` containing supported runtime functions goes through
generated C++, as does an ahead-of-time package. The REPL selects the same two
routes from the accepted session:

```text
source -> typed HIR -> hgraph IR -> direct wiring -> hgraph runtime
                              \-> C++ backend -> native -> hgraph runtime
                                  (temporary AST body/type/signature adapter)
```

Parity tests require both paths to build the same graph across their shared
subset; completing the C++ migration will make that a structural invariant.
The scripted image resolves hgraph symbols
from the running `hgl` process, so it registers into that process's registry
rather than linking a second static runtime. The compiler's parity suite holds
the shared composition subset to the same ticks and executes the runtime
subset through both the scripted and ahead-of-time compiled paths.

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
