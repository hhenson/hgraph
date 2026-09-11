# Recovered HGL design inputs

This manifest records the disposition of the earlier runtime-specification and
standard-library prototype so review work is not lost while implemented and
unresolved material move independently.

Progress checked against `main` at `36fa1113a` on 2026-09-11. The
[requirements status table](requirements.md#progress-at-a-glance) links the
merged evidence and identifies the exact boundary of each completed slice.

| Earlier input | Current location | Status |
| --- | --- | --- |
| Backend-neutral runtime model and `.hgspec` examples | Separate follow-up PR | Deliberately excluded here so the runtime specification can be reviewed as an independent design prototype. |
| Native collection view boundary | [`hgraph/native.hgl`](hgl/hgraph/native.hgl) | Compiled, installed, and tested. |
| Initial `len_` and `is_empty` source candidates | [`hgraph/standard.hgl`](hgl/hgraph/standard.hgl) | Compiled and tested as parallel operator identities; production identity and full parity remain blocked. |
| Primitive arithmetic/comparison/Boolean candidates | [`hgraph/operators.hgl`](hgl/hgraph/operators.hgl) | Supersedes the overlapping proposed implementation bodies: 16 contracts, 75 native-delegating primitive materializations, generated snapshots and runtime tests. Not production identity replacement. |
| Variadic control contracts | [`hgraph/control.hgl`](hgl/hgraph/control.hgl) | Accepted module part with `merge`, `race`, `all_`, and `any_` contracts; implementations remain native. |
| Default `when` selector fixture | [`language/examples/when-defaults.hgl`](../examples/when-defaults.hgl) | Replaced by an executable source-of-truth fixture. The obsolete design-only copy is intentionally not restored. |
| Nine standard-library operator families | [`hgl/hgraph/std`](hgl/hgraph/std) | Restored with the `.hgl.proposed` suffix for design review; excluded from all builds. |
| Migration requirements ledger | [`requirements.md`](requirements.md) | Restored with `HGL-MIG-*` identifiers to avoid colliding with compiled-library blockers. |
| Operator inventory | [`inventory.md`](inventory.md) | Restored as a historical checkpoint; current header counts still need refresh. |

The operator-family files intentionally preserve open questions. Their
`.hgl.proposed` suffix means they are not accepted compiler inputs. A
`PROVISIONAL`, `PARTIAL`, or `BLOCKED` marker is a design annotation, not a
promise that the shown spelling is valid HGL. The accepted language remains
defined by the design/user/developer guides and executable compiler examples.

## Remaining work

This is a migration checklist, not a claim that new syntax has been agreed or
an instruction to implement every deferred extension next.

- [x] Recover all nine family designs, with a separate non-compiled suffix and
  `HGL-MIG` ledger; retain the historical inventory without presenting it as a
  current coverage count.
- [x] Record implemented module parts, all three composition pack shapes,
  domain properties and fixed symbol mappings (including floor division), and
  public-contract versus candidate-local `requires`.
- [x] Link compiled native, standard, control, and operator slices to their
  source-of-truth files and generated-C++ validation boundary.
- [ ] Refresh the historical inventory against the current public headers,
  registrations, and [operator catalogue](../../docs/source/reference/operator_catalogue.rst).
  Track concrete candidates, not only operator names; classify each as HGL
  candidate, named blocker, or reviewed native kernel. Prototype signatures
  still need a declaration-by-declaration audit (defaults, parameters, policies,
  return shapes); a matching name is not full contract coverage.
- [ ] Bind generated implementations to imported production operator identities
  (`HGL-MIG-009` / `HGL-LIB-001`), including source/native name collisions.
- [ ] Resolve open downstream-type publication and body-visible generic
  reification (`HGL-MIG-015`); finish runtime pack views/constraints and
  candidate-arity refinements (`HGL-MIG-002`, `HGL-MIG-010`).
- [ ] Complete the first collection slice's startup/never-valid behavior,
  retained rolling schemas, and TSB metadata (`HGL-LIB-002`–`004`), with parity
  through public C++ and Python wiring before any production replacement.
- [ ] Agree the missing output/dependent-type, collection mutation, delta
  forwarding, generic state, and effect/capability contracts
  (`HGL-MIG-004`–`008`, `HGL-MIG-012`, `HGL-MIG-014`).
- [ ] Implement the agreed explicit `switch` and enum/conversion features;
  register their positive and negative [design fixtures](README.md#fixture-status)
  as parser/checker/backend support lands. Existing temporal `if` lowering does
  not finish explicit `switch`; ordinary collection `elements` support does not
  finish enum enumeration.
- [ ] Complete general map/reduce/mesh callable/kernel contracts and library
  coverage (`HGL-MIG-011`), reusing native child-graph lifecycle semantics.
- [ ] Add structured library documentation/compatibility metadata
  (`HGL-MIG-013`), then promote one audited family slice at a time with
  generated-C++ snapshots, behavioral parity, and a deliberate native-kernel
  or replacement decision.

### Deliberately deferred

- Automatic graph-`for` accumulator conversion to map/reduce and
  predicate-to-switch conversion. Unordered map reductions and linear,
  order-sensitive list reductions remain documented options, not implemented
  loop transformations.
- Inverse/group/field declarations, richer property-domain predicates,
  user-defined operator symbols, and optimizer trust derived from unverified
  algebraic metadata.

The previously agreed `ref`, `signal`, imported atomic values, enum numbering,
stringification, and declaration-order enumeration rules remain owned by
[type extensions](../docs/design/type-extensions.md). Explicit reference
signatures/forwarding and input-only signals have implemented slices; imported
native value types and wiring-time access below references still have gaps.
Do not fill those gaps with new source spellings in this recovery corpus.

Resolving a requirement should update its ledger entry first, then rename and
move the smallest affected candidate into compiled `.hgl` source with parity
coverage and a generated-C++ snapshot. The design corpus should not itself be
made part of the compiler acceptance suite.
