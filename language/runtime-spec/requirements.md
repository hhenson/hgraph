# Runtime-description language requirements

Status: exploratory ledger; no entry is accepted syntax or implementation

The `.hgspec` files refer to these identifiers from `PROVISIONAL` comments.
They make invented syntax and unresolved semantics searchable during review.

## RDL-001: source syntax and semantic model

The entire `.hgspec` grammar is provisional. Before it becomes a language, a
parser spike must prove deterministic parsing, qualified imports, source spans,
diagnostics, and round-trip formatting. The parsed model must distinguish
semantic contracts from backend configuration and reject executable algorithms.

## RDL-002: versioning and ABI profiles

Package compatibility, schema evolution, backend-source compatibility, and
binary ABI compatibility are different concerns. Define them separately.
Fixed-width C ABI details should live in an explicit profile rather than leak
into every semantic declaration.

## RDL-003: ownership and lifetime lowering

`owned`, `borrowed`, `inout`, and `shared` need formal transfer, aliasing, and
lifetime rules. At minimum, C++/Rust generation must prove that evaluation-local
views cannot escape, repeated node evaluation does not consume the node, and a
provider cannot unload while any plan or graph retains a lease.

## RDL-004: executable laws and scenarios

Define the finite domain over which a `law` is generated, and distinguish a
property test from a `scenario` trace. Backends must report the same observable
failure identity; the DSL is not intended to become a theorem prover.

## RDL-005: canonical identity and fingerprints

Specify canonical encoding, nominal versus structural identity, registry
generations, stable fingerprints, and collision handling. Pointer identity may
be a backend optimization but cannot be the portable contract.

## RDL-006: shared operator resolution

Operator patterns, substitutions, rejection reasons, ranking, and ambiguity
must denote the existing hgraph semantics. HGL, generated backends, and native
providers must not acquire independent resolution algorithms.

## RDL-007: generated and hand-written boundaries

Generated descriptors, façades, registration manifests, and tests must be
readable. Backend algorithms and representation strategies occupy named
extension points that regeneration never overwrites. A two-backend spike must
demonstrate this boundary before broadening the model.
