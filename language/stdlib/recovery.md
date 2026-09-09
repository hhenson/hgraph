# Recovered HGL design inputs

This manifest records the disposition of the earlier runtime-specification and
standard-library prototype so review work is not lost while implemented and
unresolved material move independently.

| Earlier input | Current location | Status |
| --- | --- | --- |
| Backend-neutral runtime model and `.hgspec` examples | Separate follow-up PR | Deliberately excluded here so the runtime specification can be reviewed as an independent design prototype. |
| Native collection view boundary | [`hgraph/native.hgl`](hgl/hgraph/native.hgl) | Compiled, installed, and tested. |
| Initial `len_` and `is_empty` source candidates | [`hgraph/standard.hgl`](hgl/hgraph/standard.hgl) | Compiled and tested as parallel operator identities; production identity and full parity remain blocked. |
| Default `when` selector fixture | [`language/examples/when-defaults.hgl`](../examples/when-defaults.hgl) | Replaced by an executable source-of-truth fixture. The obsolete design-only copy is intentionally not restored. |
| Nine standard-library operator families | [`hgl/hgraph/std`](hgl/hgraph/std) | Restored with the `.hgl.proposed` suffix for design review; excluded from all builds. |
| Migration requirements ledger | [`requirements.md`](requirements.md) | Restored with `HGL-MIG-*` identifiers to avoid colliding with compiled-library blockers. |
| Operator inventory | [`inventory.md`](inventory.md) | Restored as a historical checkpoint; current header counts still need refresh. |

The operator-family files intentionally preserve open questions. Their
`.hgl.proposed` suffix means they are not accepted compiler inputs. A
`PROVISIONAL`, `PARTIAL`, or `BLOCKED` marker is a design annotation, not a
promise that the shown spelling is valid HGL. The accepted language remains
defined by the design/user/developer guides and executable compiler examples.

## Review order

The requirements ledger is the useful entry point. The highest-leverage
decisions for replacing existing nodes and graphs are:

1. one module/provider identity across maintainable source parts
   (`HGL-MIG-001`);
2. open generic implementation publication and body-visible reification
   (`HGL-MIG-015`);
3. variadic/keyword packs and implementation arity (`HGL-MIG-002` and
   `HGL-MIG-010`);
4. output/type resolution and compiler-owned higher-order kernels
   (`HGL-MIG-008` and `HGL-MIG-011`); and
5. typed collection mutation, delta forwarding, effects, and explicit empty
   input policies (`HGL-MIG-006`, `HGL-MIG-007`, `HGL-MIG-012`, and
   `HGL-MIG-014`).

Resolving a requirement should update its ledger entry first, then rename and
move the smallest affected candidate into compiled `.hgl` source with parity
coverage and a generated-C++ snapshot. The design corpus should not itself be
made part of the compiler acceptance suite.
