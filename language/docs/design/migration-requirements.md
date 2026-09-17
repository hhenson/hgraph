# Standard-library migration requirements

Status: accepted ledger, extracted from PR #801 on 2026-09-17 and verified
against `main`. It is the definition of the `HGL-MIG-*` and `HGL-LIB-*`
identifiers that the [implementation catalogue](migration-catalogue.md),
source `TODO(HGL-LIB-...)` comments and ADR 0008 cite.

The [catalogue](../../stdlib/catalogue/README.md) answers *which operator is
blocked on what* with six blocker families (B1–B6). This ledger answers the
question one level up: which language or runtime contract each family is
waiting on, what part of it is already accepted, and which decision is still
open. A requirement is not a feature request; the catalogue does not authorize
inventing syntax for a blocked entry, and neither does this page.

## Two identifier families

- `HGL-MIG-001` … `015` name language, runtime and packaging contracts that the
  first library extraction discovered. They were numbered in PR #801 and are
  kept stable so catalogue reviews, ADRs and source comments can cite them.
- `HGL-LIB-001` … `004` name the parity blockers recorded beside the compiled
  `standard.hgl` slice. They are narrower than a `MIG` entry and each maps to
  one.

## Progress at a glance

Status vocabulary: **implemented** means the stated slice is on `main`;
**partial** names working substrate and a remaining boundary; **open** needs a
contract before implementation can start. A slice being implemented never
implies that every operator family using it has migrated.

| Requirement | Status | Catalogue blocker | Accepted record | Still open |
| --- | --- | --- | --- | --- |
| MIG-001 module parts | implemented | — | [ADR 0006](decisions/0006-multi-file-module-parts.md) | Automatic discovery and package manifests are tooling, not language. |
| MIG-002 parameter packs | implemented | B6 | [ADR 0007](decisions/0007-parameter-packs.md); stack #889–#897 merged 2026-09-11/12 | A runtime function accepts one aggregate pack input. |
| MIG-003 algebraic properties | implemented, scoped | — | [Operators](operators.md) | Claims are not optimizer permissions; inverse/group/field vocabulary deferred. |
| MIG-004 scalar/native boundary | partial | B1, B3 | [Native interface](native-interface.md), [ADR 0009](decisions/0009-native-errors-and-the-node-error-model.md), [native surface](native-surface-proposal.md) | Owned non-scalar native results; imported atomic types; typed view shapes. |
| MIG-005 recordable state | partial | B2, B4 | [ADR 0008](decisions/0008-temporal-contracts-and-target-mappings.md) settles cache vs state | Generic state without a default; sparse state; queues and windows; non-recordable cache declarations; owned native state construction. |
| MIG-006 collection mutation | implemented, node scope | B4 | [Language model](language-model.md#function-abstraction), typed C++ wrappers (#881–#885) | Graph-form mutation; live structural-child writes. |
| MIG-007 delta forwarding | open | B4 | — | Type-preserving delta value versus a dedicated forwarding effect. |
| MIG-008 output resolution | partial | B3 | Constraint language in [functions](../user-guide/functions.md#requirements-and-type-constraints) | Dependent output schemas and imported resolver metadata. |
| MIG-009 operator identity | partial | — (LIB-001) | Symbol mapping in [Operators](operators.md#fixed-symbol-to-name-mapping); imported-operator checkpoints 1–3 in the [roadmap](roadmap.md#imported-operator-migration-checkpoints) | Binding a compiled body to an imported production identity; keyword-colliding native names. |
| MIG-010 implementation arity | partial | B6 | Pack cardinality (#891–#892), ranking in ADR 0007 | Fixed candidates refining a pack contract; implementation-specific scalar parameters. |
| MIG-011 higher-order forms | partial | B6 | [Switch](switch.md), [Iteration](iteration.md), temporal `if` lowering | Explicit `switch` implementation; general map/reduce/mesh callable contracts. |
| MIG-012 effects and capabilities | partial | B1, B2, B5 | Descriptor phase/effect/ownership metadata ([native interface](native-interface.md#descriptor-is-the-contract)); throwing evaluation calls ([ADR 0009](decisions/0009-native-errors-and-the-node-error-model.md)); clock and scheduler ([ADR 0010](decisions/0010-lifecycle-capabilities.md)) | Resources, additional approved effects, a closed vocabulary shared with descriptors. |
| MIG-013 library metadata | open | — | — | Structured documentation, defaults, stability and compatibility metadata on HGL declarations. |
| MIG-014 empty input policies | partial | B2 (LIB-002) | [ADR 0010](decisions/0010-lifecycle-capabilities.md) admits scheduled handlers with an empty activation set | A source form for an explicitly empty validity set. |
| MIG-015 generic publication | partial | B3 | `instantiate` with retained `_` slots; typed native views read live metadata | Who materializes open downstream types and how a body reads a resolver-selected generic. |

| Library blocker | Maps to | What it blocks |
| --- | --- | --- |
| LIB-001 parallel identity | MIG-009 | Every compiled `hgraph.std.*` / `hgraph.operators.*` body registers as a parallel identity, never as the production overload. |
| LIB-002 startup and never-valid inputs | MIG-014, B2 | Startup scheduling is available; observing bound-but-invalid inputs still needs an explicitly empty validity set. |
| LIB-003 retained rolling extents | MIG-015, B3 | A retained rolling size lowers to an any-window pattern and cannot materialize the concrete input schema. |
| LIB-004 bundle metadata | MIG-015, B3 | TSB size/emptiness is schema metadata and needs a graph-level metadata operation rather than a live-view projection. |

## Open decisions, by requirement

Only entries with a decision not recorded elsewhere are expanded here.

### MIG-005: generic recordable state

`dedup`, `take` and `drop` fit the existing scalar `state` form. Other stream
nodes need generic state with no natural default, sparse validity, queues or
windows. The decision is between optional state cells, constructor functions,
and an admitted opaque native state type. Whatever is chosen: state that
affects later output remains recordable, and scratch caches must not be
disguised as recordable state (ADR 0008 fixes that distinction).

### MIG-007: delta capture and forwarding

`pass_through` for the eight scalar domains is compiled. The general operator
requires the complete input delta, including structural and collection
removals, applied to an output of the same temporal schema. `delta(value)` has
no result type today. The choice is a first-class, type-preserving delta value
or a dedicated forwarding effect; either must work through the public runtime
contract and in both backends.

### MIG-008: output and type resolution

Whatever expresses dependent outputs for `convert`, `combine`, `collect`,
`split`, frame joins and higher-order calls must lower to the shared hgraph
resolver. A second ranking or inference algorithm in the compiler is not
acceptable, and an unconstrained generic `O` on a contract is not a resolver.

### MIG-009: source names versus native identities

Two native identities collide with HGL source spellings: `const` is an HGL
keyword, and `default` is the fallback label of the planned `switch` form. Their
contracts are fixed by the operator markers in
`include/hgraph/lib/std/operators/conversion.h` and cannot be spelled as
declarations today:

```text
native identity "const"   (marker const_):
    Scalar<"value", ScalarVar<"T">>, TypeArg<"tp", TsVar<"S">, AutoResolve>,
    Scalar<"delay", TimeDelta>, Out<TsVar<"S">>
native identity "default" (marker default_):
    In<"ts", TsVar<"S">>, In<"default_value", TsVar<"S">>, Out<TsVar<"S">>
```

The parameter names and type relationships are part of the contract. `const`
takes an independent scalar `T`; its output shape `S` is auto-resolved from the
value or selected explicitly (`const[TS[float]](1)`), and `delay` is an optional
keyword after `tp`. `default` names its first input `ts`, so named calls such as
`default(ts=price, default_value=zero)` are established call shapes. The library
needs a reviewable mapping from a legal source declaration to the stable native
identity that preserves these names, defaults and type relationships. Renaming a
source symbol must never create a new overload family. Internal `__`-prefixed
identities (`__lag_proxy`, `__print_sink`, `__log_sink`, `__assert_fmt`,
`__apply_value_callable`, `__call_value_callable`, `__json_object`,
`__json_array`) are compiler-selected kernels; they are never public HGL names,
whatever their catalogue disposition.

### MIG-010: fixed candidates beside pack contracts

Native candidates may refine a variadic contract with a fixed arity or add
implementation-specific scalar parameters. Undefined: how an `impl fn` declares
that relationship, how a call discovers the extra parameters, and how ranking
compares fixed and packed forms. Algorithm dependencies stay on the candidate
and never leak to sibling implementations.

### MIG-012: effects and capabilities

Source-native evaluation functions remain non-blocking. ADR 0009 admits
`throws` and the descriptor's `translated` policy under hgraph's node error
model; functions without `throws` remain `noexcept`. Owned scalar results are
admitted, while owned non-scalar results remain B1. ADR 0010 implements the
clock and scheduler capabilities, scheduled activation, and evaluation-time
input activity. Non-recordable storage, input access in lifecycle hooks, and
external resource ownership remain B2. A C++ body is not permission to publish
a contract the descriptor cannot enforce: the vocabulary must be closed and
shared between HGL source, descriptors and the backend-neutral runtime
specification.

### MIG-013: library documentation and compatibility metadata

Operator documentation, parameter meanings, complexity notes, defaults and
Python examples currently live in the native headers. A migrated declaration
needs structured documentation and stability metadata from which the C++,
Python and HGL surfaces are generated, without making comments executable.

### MIG-015: open generic implementation publication

`instantiate op<A, _>` closes some positions and retains others for the
resolver. Collection length needs no body-visible reification because the
native view reads live metadata. Where a body genuinely reads the selected
value, the design must choose the owner of later materializations: the
consuming AOT module, a descriptor-backed implementation factory, or an
explicit body-availability contract on a retained candidate. The choice must
keep one operator identity, ordinary ranking, module lifecycle removal,
readable generated code and backend portability.

## Notes recovered from the family designs

The nine proposed family files in PR #801 were contract inventories; the
catalogue now derives that inventory from the C++ headers. These observations
in them are not derivable from the headers and are kept:

- `cmp_` needs a nominal result enum; range and civil policy parameters
  (`month_end_policy`, `ambiguous`, `nonexistent`) should become nominal
  enums once imported enum mapping exists. `i64` is a placeholder.
- `join` is one native identity spanning the string and frame families. It is
  a useful test that a nominal operator family can span candidates with
  different call shapes without source-order dispatch.
- Set `union` in HGL needs membership in the *other* input to handle removal,
  plus a rule for simultaneous deltas. The `contains` intrinsic on set inputs
  now exists, so this candidate deserves a fresh review under B4.
- A binary `merge` written as ordered `when` blocks makes the left input win
  when both tick; that spelling is correct but the native family also carries
  reference reselection, which is why the catalogue keeps it under B4.
- Bodies proposed for `sample`, `filter_`, `dedup`, `drop`, `null_sink`,
  `pass_through`, `min_` and `max_` have graduated into compiled source.
  `take` now has a scalar slice using evaluation-time passivation (ADR 0010);
  passivating a restored exhausted counter during `start` remains B2.
  `debug_print` remains blocked: logger formatting of an arbitrary value is B5.

## Definition of migrated

The catalogue's *implemented* means authored and parity-tested as a parallel
identity. Production replacement additionally requires the criteria in the
roadmap's [core migration programme](roadmap.md#core-standard-library-migration-programme),
plus two conditions PR #801 recorded that the roadmap does not:

- the HGL source contains no provisional syntax or design annotation; and
- the generated module registers through the same provider transaction and
  operator registry as the implementation it replaces.

## Superseded material

The historical operator inventory, the recovery manifest, and the
`.hgl.proposed` contract lists in PR #801 are superseded by the checked
catalogue and are intentionally not copied. PR #801 is closed; this page is
the surviving record.
