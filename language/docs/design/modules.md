# Modules and native extensions

Status: initial design

## Boundary

Hgraph and its native extensions provide the capabilities from which language
programs compose applications. A language module is a typed, enumerable view
of one such package. It is not a general foreign-function interface.

The compiler has one mandatory module for the hgraph kernel. Analytics,
persistence, Kafka, web, fabric, and downstream packages remain optional. An
import adds only the dependency owned by that module.

## Module contents

A native package that supports the language supplies a descriptor containing:

- canonical language module name and version;
- compatible hgraph SDK and descriptor-format versions;
- public graph, node, operator, scalar, and schema declarations;
- the C++ headers required by generated code;
- CMake package names and imported targets;
- explicit type and operator registration entry points;
- optional documentation and source links for diagnostics and tooling.

The descriptor contains declarations and build metadata, not executable user
code. The exact serialization format remains open. A checked-in textual
interface plus a small manifest is preferred for the first implementation
because it is reviewable and available before native code is loaded.

## Import and build resolution

For a declaration such as:

```text
use hgraph.analytics::{rolling_mean}
```

the compiler:

1. locates the `hgraph.analytics` descriptor through the package search path;
2. checks its hgraph and language compatibility constraints;
3. adds its public declarations to name and type resolution;
4. delegates imported operator selection to the hgraph resolver;
5. records required headers, packages, targets, and registration hooks in the
   generated build manifest.

Generated code includes only selected module headers and links only selected
module targets. Registration is explicit and deterministic; modules do not
depend on static initialization.

## One operator resolver

The compiler owns language type checking but does not own a second hgraph
overload algorithm. Imported operator calls are presented to a compiler bridge
over hgraph's `TypePattern`, `ResolutionMap`, and operator registry. The bridge
returns the selected candidate, resolved output schema, and rejected-candidate
diagnostics.

Generated C++ makes the corresponding public operator wiring call. Both paths
therefore consume the same registrations and matching rules. A mismatch between
compiler prediction and generated wiring is a compiler defect and belongs in a
cross-mode regression test.

User-defined nodes and graphs have exact typed declarations in the compilation
unit and do not need registry dispatch unless a later language feature
explicitly registers them as operator overloads.

## Native adaptor boundary

Push adaptors, services, and external-resource sinks are implemented in C++.
Their module declarations may expose graph-safe constructors or operator calls,
but language source cannot provide callbacks, queue storage, thread ownership,
or arbitrary lifecycle hooks to them.

An imported adaptor owns:

- sender admission and backpressure;
- start and stop task ownership;
- service resource sharing;
- simulation versus real-time wiring;
- protocol acknowledgement and teardown.

The language graph supplies typed ports and wiring-time policy values. Dynamic
behavior is represented by time-series inputs and explicit hgraph runtime
operators, not by escaping to native code.

## Package manifest

A language application will eventually carry a manifest which records:

- package name and language edition;
- source roots and entry graph;
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
