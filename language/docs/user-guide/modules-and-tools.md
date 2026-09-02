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

`use` imports an explicit set of public declarations:

```hgl
use hgraph.std::{if_then_else}
use hgraph.analytics::{rolling_mean, zscore}
```

The first slice has no wildcard imports or aliases. Canonical scalar and
container types, temporal-shape rules, `atomic<T>`, and expression operator
bindings come from the implicit hgraph prelude. Everything else must be
imported or declared locally.

Each import is checked against a language module descriptor. A descriptor
contains typed declarations, versions, required public headers, CMake package
and target names, and registration entry points. It does not grant access to
arbitrary symbols in a library.

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
hgl run path/to/program.hgl
hgl build path/to/program.hgl --profile release
hgl repl
```

| Command | Intended behavior |
| --- | --- |
| `check` | Parse, resolve, phase-check, and type-check without compiling |
| `run` | Compile into a content-addressed cache and execute in a child process |
| `build` | Produce a reproducible ahead-of-time native artifact |
| `repl` | Accumulate declarations and compile the current session |

The current scaffold implements only `hgl --help` and `hgl --version`. These
commands are a documented target, not yet an available interface.

## One execution model

`run`, `build`, and the REPL share:

```text
source -> checked semantic IR -> generated C++ -> native compiler -> hgraph runtime
```

The initial REPL may rebuild the whole session after each accepted declaration.
That is slower than a JIT but guarantees that exploration sees the same
function classification, overload, graph, node, and scheduling semantics as an
ahead-of-time production binary.

External input is supplied by imported native facilities or purpose-built
testing sources. A REPL convenience must not become an interpreter-only push
adaptor.
