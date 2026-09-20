# Specification consolidation

Status: proposed consolidation and backport; source inventory reviewed
2026-09-20. Supersedes the four experiments below without merging their branches.

## Review of the documentation model

Keep the six concept chapters and their progression from concept to
relationships, state, behaviour and numbered rules. This makes the model
readable without learning a second programming language. Stable rule IDs make
it possible to connect an implementation or test to the explanation that
justifies it. Runtime meaning stays independent of Rust, C++, HGL syntax and
storage choices.

The missing layer was evidence: numbered rules alone did not say which were
implemented, what a test should observe, or how conflicts were resolved. The
consolidation adds [conformance](conformance.md), finite cases,
[representation relationships](representations.md), [evidence](evidence.md)
and [boundary requirements](boundaries.md). Keep small examples in separate
files rather than turning each chapter into a testing manual.

Design, implementation and validation status remain separate. New physical
examples do not become universal runtime requirements. A source discrepancy
does not authorize an implementation change in this documentation backport.

## Sources and disposition

The chapter baseline is hgl `7f2087277fa717bc5245213efc65433a8129a848`,
`docs/runtime_spec/`. The upstream audit revision is pinned in Evidence.

| PR | Reviewed head | Disposition |
|---|---|---|
| [#796](https://github.com/hhenson/hgraph/pull/796) | `72d9ace16472ccfe43dcc45b2ad1657626469249` | Extract concepts, laws, ownership/provider requirements and backend boundaries; retire DSL/generator proposal |
| [#933](https://github.com/hhenson/hgraph/pull/933) | `ac579ab5f50755b1b5b61ae821d7e575cec0822e` | Integrate semantic distinctions, trace method and open-work ledger into this model |
| [#934](https://github.com/hhenson/hgraph/pull/934) | `e9001b6beff6fd193519bbc22e5447e1b205c0b0` | Translate all seven finite scenarios and six layout cases into prose/tables; preserve bounded handoff requirements |
| [#937](https://github.com/hhenson/hgraph/pull/937) | `514370b5d29181e039ac3262746ce99cd262a510` | Preserve quote history, map/pivot eligibility, complete state relations and evidence invalidation |

The old branches and their commit-linked files remain provenance. No parser,
generator, native layout or runtime change is adopted by closing them. The
unrelated CI PR and value-text-rendering RFC are outside this consolidation.

## File-level extraction

Paths in this table are relative to hgraph. Repeated README and syntax files
are evaluated at the final stack head as well as through each PR's diff.

| Source file(s) | Retained destination or reason not adopted |
|---|---|
| #796 `language/README.md`, `language/docs/README.md` | Navigation is replaced by a link to the integrated upstream specification, not to an experiment |
| #796 `language/docs/design/runtime-specification.md` | Concept/implementation split, provider retention, identity and generation boundaries in Boundaries; provisional grammar is retired |
| #796 `language/runtime-spec/README.md` | Scope and status in Overview and this ledger |
| #796 `language/runtime-spec/requirements.md` | RDL-001 grammar deferred; RDL-002 version/ABI, RDL-003 lifetime, RDL-005 identity, RDL-006 resolution, RDL-007 generation in Boundaries; RDL-004 laws/traces in Conformance |
| #796 `language/runtime-spec/backends.md` | C/C++/Rust/Swift mapping and C ABI boundary retained in Boundaries; no backend or generator mandated |
| #796 `language/runtime-spec/spec/values.hgspec` | Scalar chapter, logical identity/capabilities and ownership in Boundaries; implementation flags are not portable type identity |
| #796 `language/runtime-spec/spec/time-series.hgspec` | Time-series chapter, collection cases and view lifetimes; signal remains an input observation, generic delta helper and weak time law not adopted |
| #796 `language/runtime-spec/spec/execution.hgspec` | Engine, graph, node and injectable chapters; absent-versus-empty policies; provider lifetime in Boundaries |
| #796 `language/runtime-spec/spec/operators.hgspec` | Callable/candidate distinction, ranking vectors, ambiguity and registration requirements in Boundaries; incomplete helper names are not an algorithm |
| #796 `language/runtime-spec/spec/conformance.hgspec` | Scalar trace in Atomic; set trace in Collections; ranked activation in Lifecycle; provider-removal trace in Boundaries |
| #933 `language/runtime-spec/README.md` | Overview, status/evidence distinction and this review |
| #933 `language/runtime-spec/core-model.md` | Six chapters plus Boundaries; state/cache equivalence, type relations and storage independence preserved |
| #933 `language/runtime-spec/conformance.md` | Conformance and ATOMIC-RETENTION; exact observations, progress and failure boundaries retained |
| #933 `language/runtime-spec/extraction-and-decisions.md` | This ledger, Evidence and the OPEN-01/03–12 disposition in Boundaries |
| #934/#937 `language/runtime-spec/README.md`, `extraction-and-decisions.md` | Updated experiment scope and decisions folded into this review and status model |
| #934/#937 `language/runtime-spec/notation/README.md` | Review method in Conformance; atomic, ownership, validity and representation explanations in their own case/profile files |
| #934/#937 `language/runtime-spec/notation/syntax.md` | Preconditions versus rejection versus partial failure, observation points, complete relations and scoped evidence retained; grammar/lexer/expression syntax retired |
| #934/#937 `language/runtime-spec/notation/examples/atomic.hgspec` | ATOMIC-RETENTION, ATOMIC-ZERO, AtomicStorage and AtomicCell in Atomic and Layout example |
| #934 `language/runtime-spec/notation/examples/owner.hgspec` | PAIR-NORMAL, PAIR-CONSTRUCTION-FAILURE, PAIR-NEVER-STARTED and PairStorage; all original ordered cleanup and rejection outcomes retained |
| #934 `language/runtime-spec/notation/examples/validity.hgspec` | VALIDITY-IMMEDIATE replaces NestedValidity and DictionaryDoesNotRecurse; corrects stale TSD formula and adds invalid-live-child boundary |
| #934/#937 `language/runtime-spec/notation/examples/atomic-slice.hgspec` | Bounded handoff in Conformance, physical checks in Layout example; speculative implementation paths are not prescribed |
| #937 `language/runtime-spec/notation/representations.md` | Representations: independent contracts, composition, plan selection and evidence registry |
| #937 `language/runtime-spec/notation/examples/tsd-behavior.hgspec` | QUOTE-HISTORY, preserving current, delta and all four time projections; TS-9 replaces the stale root-valid shortcut |
| #937 `language/runtime-spec/notation/examples/tsd-storage.hgspec` | Exact finite NodeMap/FlatPivot predicates and all eight eligibility cases in Representations |
| #937 `language/runtime-spec/notation/examples/tsd-realizations.hgspec` | Every abstract state fact, action, initialization and observation obligation in the realization table |

## Corrections and deliberate exclusions

The old TSD validity shortcut is not carried over; current C++ and the newer
chapter agree on immediate live children. Nil, never, unmodified, absent key
and invalid child remain distinct observations. The quote case's removal view
does not erase TS-11's removed-child lifetime. Current recursive HGL support
replaces the outdated blanket limitation in the scalar chapter.

The chapter's reversed input/output cardinality and per-input notification
stamp were editorial/model defects and are corrected. Timestamp derivation is
scoped to owned outputs so sampled inputs and keyed withdrawal are not silently
contradicted. Cache equivalence is stated with authoritative state and pending
work as preconditions, not as a certification of arbitrary native State use.

The unresolved #934 [newline/braces review](https://github.com/hhenson/hgraph/pull/934#discussion_r4002700117)
concerns its proposed parser grammar. That grammar is not retained; prose and
Markdown tables remove the defect's subject. Do not label the old parser rule
fixed in place or transfer an unresolved syntax rule into the new model.

No source grammar, fixed enum discriminants, canonical fingerprint algorithm,
universal ops table, universal allocation guarantee or cross-language ABI is
selected. The 16/32-byte profiles are bounded proposals. Special-node,
operator, recovery and provider work is preserved as named boundary work,
not lost and not asserted complete. Original rendering/check counts remain
historical evidence only.

## Upstream ownership

The backport places this model in hgraph `docs/source/runtime_spec/` with
normal documentation navigation. The Python-era `docs/source/specification/`
chapters remain historical reference for domains not covered here. HGL source
syntax continues to live in `language/docs/`.

This folder in hgl is a synchronized working copy for the runtime experiment.
Behaviour changes are proposed upstream first; counterpart chapters and cases
must be updated together. Validate relative links, rule IDs, case arithmetic
and document rendering before publication. Close the four source PRs only
after the extracted backport is committed and available for review; do not
merge them or delete their provenance branches.
