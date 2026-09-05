# Native interface

Status: accepted boundary; descriptor spelling and ABI remain to be implemented

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
- complete HGL parameter and result types;
- permitted phases: wiring, start, evaluation, or stop;
- observable effects, including mutation, I/O, blocking, and allocation where
  relevant;
- value, owned, shared, or borrowed ownership and any dependent lifetime;
- exception and thread-safety policy;
- canonical C++ symbol or generated wrapper identity;
- required public headers, CMake packages, imported targets, and runtime image;
- module lifecycle entry points, provider identity, compatibility versions, and
  descriptor fingerprint.

The serialized representation and authoring API for the descriptor are not yet
chosen. No HGL declaration syntax is implied by this list.

## Native declaration categories

### Hgraph operators

A temporal callable is a normal registered hgraph operator. The descriptor
exposes its nominal operator contract and provider candidates, and HGL resolves
it through the shared hgraph resolver. This is the path for graphs, nodes,
sources, sinks, adaptors, and services that accept temporal arguments.

An ordinary C++ scalar function is never lifted implicitly into one node per
call. A package that wants temporal use supplies and registers the corresponding
hgraph operator implementation explicitly.

### Exact native value functions

An exact native value function is callable only in phases allowed by its
descriptor. The first implementation targets scalar computations inside an HGL
runtime node and construction or cleanup of that node's private native state.

The generated C++ calls the declared symbol or its package-provided wrapper
directly. The direct-wiring backend does not emulate it: a runtime-bearing
program follows the existing generated, compiled, and loaded image path.

Calls from wiring-time constant evaluation, automatic temporal lifting, and
general compile-time execution are outside the first interface.

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

- arguments and results are canonical scalar values or an opaque state value
  declared by the same module;
- opaque state uses owned RAII storage and cannot cross a temporal port;
- evaluation functions are non-blocking and `noexcept`;
- mutation is restricted to an explicitly identified state argument;
- raw pointers, references, pointer arithmetic, callbacks, variadic calls, and
  open C++ templates are not representable;
- a C++ template is exposed only through an explicit specialization or wrapper
  with one complete HGL signature;
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

    when modified(value) and valid(value) {
        previous = stats::update(previous, value, window)
        return previous
    }
}
```

This example introduces no new HGL syntax. The source spelling and inference
rules for an imported opaque state type are not settled, so this record does not
invent an example for them. The native implementation must stop at that design
question if existing nominal type syntax is insufficient.

## Producing descriptors

The first producer should be an explicit C++ registration API or build-time
tool owned by the native package. It emits both the reviewable descriptor and
any wrapper required to normalize C++ overloads, templates, exceptions, or
ownership into the declared HGL contract.

An optional Clang-based binding generator may later derive the same artifact
from annotated public headers. Clang is then a descriptor-generation tool, not
part of HGL parsing or the definition of which arbitrary C++ constructs the
language accepts. The generated descriptor remains reviewable and versioned.

## Module lifecycle and ABI

The descriptor participates in the existing closed package universe. Its
provider initializes transactionally, installs all registrations through one
module-owned handle, and deinitializes in reverse dependency order. Graphs,
plans, native call targets, and metadata retain provider leases.

The dynamic entry point used for descriptor verification and lifecycle should
use a small versioned ABI that can be discovered reliably across toolchains.
Calls within generated code may still use direct C++ types and functions when
the descriptor permits them. Logical provider removal and native-image
unloading remain distinct operations.

## Acceptance

The first native package proves:

- descriptor-only `hgl check` without loading its library;
- one canonical scalar value function used inside a runtime node;
- one owned opaque state value constructed at startup, mutated during
  evaluation, and destroyed after stop;
- rejection of the same calls in an unpermitted phase;
- rejection of a borrowed value that escapes;
- generated C++ that is a direct, readable call through public headers;
- scripted and ahead-of-time execution with identical ticks;
- descriptor/provider fingerprint mismatch before graph wiring;
- failed activation rollback and provider removal without stale registrations;
- an installed-SDK consumer build, not only an in-tree test.
