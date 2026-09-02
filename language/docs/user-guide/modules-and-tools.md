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

There are no wildcard imports. Canonical scalar and container types,
temporal-shape rules, `atomic<T>`, and expression operator bindings come from
the implicit hgraph prelude. Everything else must be imported, reached through
a module alias, or declared locally.

## Operator identity and implementation imports

An operator is identified by its defining module and name, not by its short
name alone. The canonical identities `market.pricing::value` and
`risk.pricing::value` therefore denote distinct contracts. Canonical identities
appear in diagnostics and metadata; source calls qualify through a local module
alias rather than spelling a dotted module path as an expression.

An implementation module binds same-named `fn` definitions to a local operator
declaration or to exactly one operator brought into local scope by a selective
import:

```hgl
module market.pricing_impl

use market.pricing::{value}

fn value(input: f64) -> f64 =>
    input
```

The uniqueness rule applies per local short name. Selectively importing two
different operator definitions as `value` is an import error before function
checking. A module alias does not establish an implementation binding, so an
implementation module may still use other same-named operators explicitly:

```hgl
module market.pricing_impl

use market.pricing::{value}
use risk.pricing as risk

fn value(input: f64) -> f64 =>
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

Each import is checked against a language module descriptor. A descriptor
contains typed declarations, nominal operator identities, versions, required
public headers, CMake package and target names, and registration entry points.
It does not grant access to arbitrary symbols in a library.

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
