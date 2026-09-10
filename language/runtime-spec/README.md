# hgraph runtime specification prototype

Status: exploratory; syntax and file layout are provisional

This folder tests a small backend-neutral DSL for describing hgraph runtime and
type-generation contracts. It complements HGL rather than extending it:

- `.hgspec` describes the portable runtime contract;
- a C, C++, Rust, Swift, or other backend implements that contract;
- `.hgl` describes graph and node libraries which target the contract.

Nothing in this folder is consumed by CMake or the HGL compiler. Every source
file begins with a `PROVISIONAL` notice. The design and intended generation
boundary are in the
[backend-neutral runtime specification](../docs/design/runtime-specification.md).

## Prototype files

- [`values.hgspec`](spec/values.hgspec) describes canonical value schemas,
  capabilities, storage, and erased value operations.
- [`time-series.hgspec`](spec/time-series.hgspec) describes temporal shapes,
  endpoint state, deltas, and evaluation-local collection views.
- [`execution.hgspec`](spec/execution.hgspec) describes node/graph lifecycle,
  scheduling, providers, and leases.
- [`operators.hgspec`](spec/operators.hgspec) describes nominal operator
  contracts, candidates, resolution, and registration transactions.
- [`conformance.hgspec`](spec/conformance.hgspec) records portable behavioral
  traces the generated backends must share.
- [`backends.md`](backends.md) sketches mappings for C, C++, Rust, and Swift.
- [`requirements.md`](requirements.md) is the searchable ledger for every
  provisional language and generation decision.

## Guardrails

- Describe semantic data and operations, not one backend's member layout.
- Keep algorithms and performance representations in backend code.
- Make ownership, borrowed lifetimes, fallibility, and lifecycle explicit.
- Generate readable contracts and tests; never generate opaque implementation
  machinery that cannot be reviewed.
- Add source constructs only in response to a real HGL library or runtime
  migration requirement.
