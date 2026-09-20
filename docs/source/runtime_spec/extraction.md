# Extraction record

Status: consolidated. The four source PRs are closed without merging;
branches are retained. Reviewed 2026-09-20.

The original chapter model stays: concept, relationships, state, behaviour,
rules. The additions supply cases, evidence and representation examples.
[Conformance](conformance.md) gives the method; [Evidence](evidence.md) records
implementation differences; [Boundaries](boundaries.md) keeps deferred work.

Baseline: hgl `7f2087277fa717bc5245213efc65433a8129a848`, `docs/runtime_spec/`.

| PR | Reviewed head | Disposition |
|---|---|---|
| [#796](https://github.com/hhenson/hgraph/pull/796) | `72d9ace16472ccfe43dcc45b2ad1657626469249` | Concepts, ownership, providers and backend boundaries |
| [#933](https://github.com/hhenson/hgraph/pull/933) | `ac579ab5f50755b1b5b61ae821d7e575cec0822e` | Semantic model, traces and open questions |
| [#934](https://github.com/hhenson/hgraph/pull/934) | `e9001b6beff6fd193519bbc22e5447e1b205c0b0` | Seven scenarios, six layout cases and handoff requirements |
| [#937](https://github.com/hhenson/hgraph/pull/937) | `514370b5d29181e039ac3262746ce99cd262a510` | Quote history, map/pivot relations and evidence |

## Where the material went

Paths are relative to hgraph. Repeated files were reviewed through the stack.

| Source file(s) | Retained destination or reason not adopted |
|---|---|
| #796 `language/README.md`, `language/docs/README.md` | Upstream specification navigation |
| #796 `language/docs/design/runtime-specification.md` | Boundaries; grammar retired |
| #796 `language/runtime-spec/README.md` | Overview and this record |
| #796 `language/runtime-spec/requirements.md` | RDL-001 deferred; RDL-004 in Conformance; RDL-002/003/005–007 in Boundaries |
| #796 `language/runtime-spec/backends.md` | Boundaries: backend and ABI requirements |
| #796 `language/runtime-spec/spec/values.hgspec` | Scalar types and Boundaries: identity, capabilities, ownership |
| #796 `language/runtime-spec/spec/time-series.hgspec` | Time-series and collection cases; generic delta helper and weak time law dropped |
| #796 `language/runtime-spec/spec/execution.hgspec` | Engine, Graph, Node, Injectables; providers in Boundaries |
| #796 `language/runtime-spec/spec/operators.hgspec` | Boundaries: candidates, resolution and registration |
| #796 `language/runtime-spec/spec/conformance.hgspec` | Atomic, Collections, Lifecycle; provider removal in Boundaries |
| #933 `language/runtime-spec/README.md` | Overview and Evidence |
| #933 `language/runtime-spec/core-model.md` | Six chapters and Boundaries |
| #933 `language/runtime-spec/conformance.md` | Conformance and ATOMIC-RETENTION |
| #933 `language/runtime-spec/extraction-and-decisions.md` | This record, Evidence and Boundaries |
| #934/#937 `language/runtime-spec/README.md`, `extraction-and-decisions.md` | Overview and this record |
| #934/#937 `language/runtime-spec/notation/README.md` | Conformance, cases and representation examples |
| #934/#937 `language/runtime-spec/notation/syntax.md` | Conformance and Representations; grammar retired |
| #934/#937 `language/runtime-spec/notation/examples/atomic.hgspec` | Atomic cases and Layout example |
| #934 `language/runtime-spec/notation/examples/owner.hgspec` | All three PAIR cases in Lifecycle; PairStorage in Layout example |
| #934 `language/runtime-spec/notation/examples/validity.hgspec` | VALIDITY-IMMEDIATE; corrected TSD rule and invalid-child case |
| #934/#937 `language/runtime-spec/notation/examples/atomic-slice.hgspec` | Conformance card and Layout checks; speculative file paths dropped |
| #937 `language/runtime-spec/notation/representations.md` | Representations |
| #937 `language/runtime-spec/notation/examples/tsd-behavior.hgspec` | QUOTE-HISTORY with current TS-9 validity |
| #937 `language/runtime-spec/notation/examples/tsd-storage.hgspec` | Representations: predicates and all eight eligibility cases |
| #937 `language/runtime-spec/notation/examples/tsd-realizations.hgspec` | Representations: complete state and action relation |

## Corrections

TSD checks immediate live children. Recursive HGL fields are supported within
ADR 0012's domain. The chapters correct input/output cardinalities, input
notification state and the scope of timestamp rules. Cache reconstruction
requires equivalent authoritative state and pending work.

The provisional grammars are retired, including #934's unresolved
[newline/braces rule](https://github.com/hhenson/hgraph/pull/934#discussion_r4002700117).
No generator, ABI, fingerprint scheme, ops table or universal allocation rule
is selected. Physical examples remain proposals; old check counts are historical.

## Ownership

hgraph `docs/source/runtime_spec/` owns the model; hgl keeps a synchronized
working copy. Propose behaviour changes upstream and update both together.
The Python-era specification remains historical reference. HGL syntax stays
in `language/docs/`. The unrelated CI PR and value-rendering RFC are unchanged.
