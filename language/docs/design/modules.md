# Modules and native extensions

Status: initial design

## Boundary

Hgraph and its native extensions provide the capabilities from which language
programs compose applications. A language module is a typed, enumerable view
of one such package. It is not a general foreign-function interface.

The compiler has one mandatory module for the hgraph kernel. Its implicit
prelude supplies canonical types, temporal-shape mappings, `atomic<T>`, and the
standard operator bindings used by expression syntax. Analytics, persistence,
Kafka, web, fabric, and downstream packages remain optional.

## Module contents

A native package that supports the language supplies a descriptor containing:

- canonical language module name and version;
- compatible hgraph SDK and descriptor-format versions;
- public function and nominal operator contracts, implementation candidates,
  implementation kinds, canonical types, and schema declarations;
- the C++ headers required by generated code;
- CMake package names and imported targets;
- explicit type and operator registration entry points;
- optional documentation and source links for diagnostics and tooling.

The kernel descriptor additionally maps language operator tokens such as `+`
and `==` to public hgraph operator markers. This is a syntax binding into the
same registry, not a compiler-owned implementation.

The descriptor contains declarations and build metadata, not executable user
code. The exact serialization format remains open. A checked-in textual
interface plus a small manifest is preferred for the first implementation
because it is reviewable and available before native code is loaded.

One descriptor question must be resolved before imported operator checking:
some native candidates have scalar-dependent `requires` predicates. The
descriptor must either express those constraints declaratively for hgraph's
shared resolver or identify a controlled resolver helper that evaluates them
outside the compiler process. `hgl check` must not approximate or silently omit
such a predicate.

## Import and build resolution

Selective imports introduce declarations as unqualified local names:

```text
use hgraph.analytics::{rolling_mean}
```

A module alias introduces only a namespace:

```text
use hgraph.analytics as analytics

analytics::rolling_mean(price, 20)
```

There are no wildcard imports. Aliasing permits two modules to expose the same
short declaration name without conflating their nominal identities.

For either form, the compiler:

1. locates the `hgraph.analytics` descriptor through the package search path;
2. checks its hgraph and language compatibility constraints;
3. adds its public declarations to name and type resolution;
4. delegates candidate selection for each resolved nominal operator to the
   hgraph resolver;
5. records required headers, packages, targets, and registration hooks in the
   generated build manifest.

Generated code includes only selected module headers and links only selected
module targets. Registration is explicit and deterministic; modules do not
depend on static initialization.

## Nominal operator binding

An operator's canonical identity is its defining module plus declaration name.
Two modules may define `my_op`, but those definitions describe distinct
protocol-like contracts and own distinct implementation sets.

A same-named `fn` becomes an implementation of an operator only when its module
declares that operator locally or selectively imports exactly one such operator
into the local scope:

```hgl
module my.implementation

use my.contracts::{my_op}

fn my_op(value: f64) -> f64 =>
    value
```

The uniqueness rule applies per local short name. Two selective imports that
would bind different nominal operators as `my_op` are rejected during import
resolution. An aliased module does not create an implementation binding, so
other definitions remain callable through qualified names such as
`other::my_op(...)`.

The semantic IR records the canonical operator identity on every implementation
candidate and operator call. It never reconstructs that identity later from a
short string. A descriptor for a native package maps the canonical language
identity to its public C++ marker and registration hook. Source-defined
operators receive deterministic generated marker identities.

## One operator resolver

The compiler owns language name resolution and type checking but does not own a
second hgraph overload algorithm. Name resolution first chooses exactly one
nominal operator. The candidates registered for that identity are then
presented to a compiler bridge over hgraph's `TypePattern`, `ResolutionMap`, and
operator registry. The bridge returns the selected candidate, resolved output
schema, and rejected-candidate diagnostics.

Generated C++ makes the corresponding public operator wiring call. Both paths
therefore consume the same registrations and matching rules. A mismatch between
compiler prediction and generated wiring is a compiler defect and belongs in a
cross-mode regression test.

User-defined functions without a local operator binding retain exact typed
declarations and do not form overload sets. Same-named functions with an
operator binding are registered as that operator's candidates. Their source
bodies still determine whether each candidate lowers as composition or a
runtime node.

Namespace resolution is not candidate ranking. Different nominal operators
with the same short name never share a candidate set. Within one selected
operator, hgraph specificity rules choose the best implementation and an
equal-ranked overlap remains an ambiguity error. Declaration and registration
order never break the tie. Cross-module coherence rules for overlapping source
implementations remain to be designed.

## Native adaptor boundary

Push adaptors, services, and external-resource sinks are implemented in C++.
Their module declarations expose typed function contracts, but language source
cannot provide callbacks, queue storage, thread ownership, or arbitrary
lifecycle hooks to them.

An imported adaptor owns:

- sender admission and backpressure;
- start and stop task ownership;
- service resource sharing;
- simulation versus real-time wiring;
- protocol acknowledgement and teardown.

The language function supplies temporal arguments and `const` policy values.
Dynamic behavior is represented by temporal inputs and explicit hgraph runtime
functions, not by escaping to native code.

## Package manifest

A language application will eventually carry a manifest which records:

- package name and language edition;
- source roots and entry function;
- direct language module dependencies and version constraints;
- build profiles and target platforms;
- explicitly selected runtime mode and deployment packaging.

A lock file will pin descriptor packages, hgraph, native extensions, and the
native compiler toolchain inputs required for a reproducible production build.
Neither format is fixed by the initial scaffold.

## Development hosting

While hosted in this repository, `language/` is an independent CMake project.
The root build includes it only with `HGRAPH_BUILD_LANGUAGE=ON`. A standalone
configure finds an installed `hgraph` package in the same manner as a native
extension consumer.

The language directory is excluded from the core source distribution. When it
is packaged, it will own its distribution metadata, version, changelog, tests,
and release artifacts.
