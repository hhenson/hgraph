# ADR 0004: Module descriptors use canonical versioned JSON

Status: accepted; writer, strict reader, descriptor-only validation, native
declaration metadata, canonical fingerprints, and lifecycle ABI implemented

## Context

The native boundary selected in ADR 0003 needs one artifact which package
authors can review, build tools can transport, and `hgl check` can eventually
consume without loading executable code. The representation must not depend on
C++ object layout, compiler ABI, parser internals, or a running hgraph registry.

The first producer is the HGL C++ backend itself. The descriptor must carry
enough structured source interface data for a later reader to check imports
without reparsing HGL or loading executable code.

## Decision

Use UTF-8 JSON with the format identity `hgl.module` and an integer
`format_version`. Version 1 begins with:

- module identity and HGL release version;
- automatically public operators, exported structures, and exported functions;
- generic and ordinary parameters, results, default values, struct parents and
  effective fields;
- canonical type, compile-time-expression, and constraint records reachable
  from those public declarations and requested implementation candidates;
- implementation-to-operator bindings and required provider identities;
- generated public headers, known CMake packages and imported targets; and
- the generated C++ registration symbol.

The canonical emitter uses a fixed object-member order, lexically sorts and
deduplicates set-like identity and build inventories, preserves semantic operand
order inside expressions, escapes every JSON control character, and writes one
trailing line feed. A reader must treat object-member order and insignificant
whitespace as irrelevant, reject duplicate members and unsupported format
versions, and ignore unknown members within a supported version. Adding an
optional member is compatible; changing or removing existing meaning requires a
new `format_version`.

Declarations and candidates refer into three descriptor-local schema arenas:
`types`, `constant_expressions`, and `constraints`. Records are assigned by
visiting exported structures, local operators, exported functions, and provider
implementations in that order, with each group ordered by stable identity.
References are unsigned JSON integers and a missing optional reference is
`null`; record IDs have no meaning outside their descriptor. Generic type and
const parameters also retain a declaration-scoped binding identity, so repeated
names from different contracts never alias.

Integer and floating-point literal payloads are tagged strings. This preserves
the full i64 range and HGL's `inf`, `-inf`, and `nan` values without depending on
a JSON consumer's numeric range or its handling of non-standard JSON tokens.
Booleans remain JSON booleans, strings remain strings, and temporal values use
their canonical HGL spelling together with their temporal kind.

Descriptor fingerprints are `sha256:` followed by the lowercase SHA-256 digest
of the canonical version-one semantic model with
`module.descriptor_fingerprint` empty. Arbitrary input whitespace and object
ordering therefore do not affect the fingerprint. Compatible unknown version-one
members are outside that projection; a new field that changes the bindable or
executable contract requires a descriptor format-version increment.

The generated file is named `<stem>.hgl-module.json`. With split C++ output it
is placed beside the generated source. `hgl_add_module()` exposes the complete
list through the `HGL_MODULE_DESCRIPTORS` target property so a package can
install or aggregate it deliberately.

## Consequences

- Descriptors are human-readable and tool-neutral.
- `hgl check <file>.hgl-module.json` validates the envelope, schema record
  shapes, and every descriptor-local reference without loading native code.
- Serialization is a backend over HGraph IR, independent of parsing, C++
  formatting, registry access, and dynamic loading.
- Scripted compilation and AOT generation retain the identical descriptor
  bytes with their other build artifacts.
- The file now contains structured HGL signatures, struct layouts, defaults,
  generic bindings, and constraints. The reader ignores compatible unknown
  members but rejects duplicate keys, malformed records, unsupported versions,
  and dangling references. The native section records type associations, exact
  symbols, phase/effect/ownership/lifetime policy, exception and thread-safety
  policy; build metadata records runtime images and the separate lifecycle ABI.
  The installed native-package authoring API produces and validates this same
  representation. Locked dependency closure remains Stage F work.

## Alternatives

- C++ constexpr tables: rejected as ABI- and compiler-facing rather than a
  reviewable descriptor-only input.
- A custom binary encoding: deferred until size or load-time evidence outweighs
  the value of a directly inspectable package artifact.
- Reusing an HGraph IR dump: rejected because it includes body/compiler details
  and does not define a stable public schema.
