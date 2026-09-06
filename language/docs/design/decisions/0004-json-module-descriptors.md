# ADR 0004: Module descriptors use canonical versioned JSON

Status: accepted; reader and complete interface schema pending

## Context

The native boundary selected in ADR 0003 needs one artifact which package
authors can review, build tools can transport, and `hgl check` can eventually
consume without loading executable code. The representation must not depend on
C++ object layout, compiler ABI, parser internals, or a running hgraph registry.

The first producer is the HGL C++ backend itself. It needs to establish the
envelope and package inventories before the following slices add complete type,
constraint, lifecycle, and fingerprint records.

## Decision

Use UTF-8 JSON with the format identity `hgl.module` and an integer
`format_version`. Version 1 begins with:

- module identity and HGL release version;
- automatically public operators, exported structures, and exported functions;
- implementation-to-operator bindings and required provider identities;
- generated public headers, known CMake packages and imported targets; and
- the generated C++ registration symbol.

The canonical emitter uses a fixed object-member order, lexically sorts and
deduplicates set-like arrays, escapes every JSON control character, and writes
one trailing line feed. A reader must treat object-member order and insignificant
whitespace as irrelevant, reject duplicate members and unsupported format
versions, and ignore unknown members within a supported version. Adding an
optional member is compatible; changing or removing existing meaning requires a
new `format_version`.

Descriptor fingerprints will be computed over the canonical semantic form, not
over arbitrary input whitespace. The fingerprint field itself is excluded from
that input. Its algorithm and placement are intentionally left to the
fingerprint slice.

The generated file is named `<stem>.hgl-module.json`. With split C++ output it
is placed beside the generated source. `hgl_add_module()` exposes the complete
list through the `HGL_MODULE_DESCRIPTORS` target property so a package can
install or aggregate it deliberately.

## Consequences

- Descriptors are human-readable and tool-neutral.
- Serialization is a backend over HGraph IR, independent of parsing, C++
  formatting, registry access, and dynamic loading.
- Scripted compilation and AOT generation retain the identical descriptor
  bytes with their other build artifacts.
- The initial file is not yet sufficient for descriptor-only type checking;
  signatures, constraints, ownership/effects, lifecycle ABI, and fingerprints
  remain explicit Stage F work.

## Alternatives

- C++ constexpr tables: rejected as ABI- and compiler-facing rather than a
  reviewable descriptor-only input.
- A custom binary encoding: deferred until size or load-time evidence outweighs
  the value of a directly inspectable package artifact.
- Reusing an HGraph IR dump: rejected because it includes body/compiler details
  and does not define a stable public schema.
