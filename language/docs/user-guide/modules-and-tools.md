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
is public only when declared `export fn`; an unexported function remains
module-internal. Both selective and aliased imports expose only this public
declaration surface.

There are no wildcard imports or re-exports. In particular, an implementation
module does not create a second import path for an operator defined elsewhere.
Canonical scalar and container types, temporal-shape rules, `atomic<T>`, and
expression operator bindings come from the implicit hgraph prelude. Everything
else must be imported, reached through a module alias, or declared locally.

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
hgl check path/to/program.hgl
hgl test path/to/program.hgl
hgl run path/to/program.hgl [--entry name] [--mode sim|realtime]
        [--start <datetime>] [--end <datetime|duration>]
        [--set name=<constant expression>]... [--config run.toml]
hgl build path/to/program.hgl --profile release
hgl repl
```

| Command | Intended behavior |
| --- | --- |
| `check` | Parse, resolve, phase-check, and type-check without compiling |
| `test` | Run the module's `test` declarations and report failing assertions |
| `run` | Bind an entry to a mode, clock, and parameters, then execute it |
| `build` | Produce a reproducible ahead-of-time native artifact |
| `repl` | Accumulate declarations, run tests and `eval` forms interactively |

[Testing and running](testing-and-running.md) shows `test`, `run`, and the
run configuration file from the author's side.

The current scaffold implements only `hgl --help` and `hgl --version`. These
commands are a documented target, not yet an available interface.

## One execution model

`test`, `run`, `build`, and the REPL share one checked semantic IR and
one hgraph runtime. A program made only of composition functions is wired
onto the runtime directly, in process; a program with runtime functions goes
through generated C++:

```text
source -> checked semantic IR -> direct wiring          -> hgraph runtime
                              -> generated C++ -> native -> hgraph runtime
```

Both paths build the same graph, and the compiler's parity suite holds them
to the same ticks.

The initial REPL may rebuild the whole session after each accepted declaration.
That is slower than a JIT but guarantees that exploration sees the same
function classification, overload, graph, node, and scheduling semantics as an
ahead-of-time production binary.

When a REPL module changes, the driver stops graphs using its old revision,
removes that revision's registration handle, initializes the replacement, and
rebuilds from the resulting active module set. Removed candidates must not
survive through an installer replay.

External input is supplied by imported native facilities or purpose-built
testing sources. A REPL convenience must not become an interpreter-only push
adaptor.
