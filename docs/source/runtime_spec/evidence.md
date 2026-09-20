# Evidence and compatibility

Status: source audit at hgraph `51b66437af4fa634af2f8f017be721e4ae47a353`
(2026-09-20). Sources and tests were read; runtime suites were not executed
for this documentation change. An audit is not a conformance certification.

The six chapters describe intended behaviour. Their status does not mean
every rule is implemented. Current C++ rulings and accepted design records
take precedence over older Python-era prose; source/test evidence establishes
what is implemented, and a difference from intent must be recorded explicitly.
An old PR's label "established" is insufficient when newer evidence differs.

| Subject | Evidence at the audited revision | Consequence |
|---|---|---|
| Immediate-child `all_valid` | [Static-node tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_static_node.cpp), cases “all-valid on a TSD checks immediate children without recursion” and “dictionary all-valid tracks invalidation and removed slots”; [slot operations](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/src/hgraph/types/metadata/ts_data_slot_ops.cpp), `tsd_all_valid` | TSD checks its own validity and immediate live children. The older PR rule equating TSD all_valid with valid is superseded. No recursive descent. |
| Dictionary membership versus publication | Same slot operations, `insert_key`, `remove_key`, `record_child_modified`, `membership_added_`, `membership_removed_`, and `value_published_` | Current C++ tracks both surfaces. TS-19 names membership. A child becoming valid/invalid changes published-value delta bookkeeping without adding/removing its key. Adapters must identify which surface they read. |
| Storage-independent identity | [Type registry tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_type_registry.cpp), “a storage category takes no part in type resolution” and qualified nominal identity cases | Preserve logical identity independently of a chosen storage profile. Portable encoding remains unspecified here. |
| Activation without sampling | [Input tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_ts_input.cpp), “activation subscribes without notifying for an already-valid target” | Making active alone does not schedule. Runtime sampling is a different event. A per-input notification stamp is not part of the conceptual state. |
| Scheduler boundary | [Scheduler tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_node_scheduler.cpp), startup, tagged replacement, cancellation and wall-clock cases | Do not infer a graph-scheduling error policy from node-scheduler behaviour. The chapters retain that distinction and their open mismatch. |
| Recursive scalar fields | [ADR 0012](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/language/docs/design/decisions/0012-recursive-struct-fields.md) | Accepted and implemented in both HGL backends for the admitted domain; importing a struct from another module still waits for general struct imports. The earlier blanket “HGL rejects recursive fields” is stale. |
| State/cache | [ADR 0008](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/language/docs/design/decisions/0008-temporal-contracts-and-target-mappings.md) | Cache reconstruction is intended semantics; C++ supports State plus RecordableState, but mixed HGL lowering remains separate work. The name State alone does not prove arbitrary native history reconstructible. |

## Boundaries that remain visible

- A storable graph description is a design target (GRF-1), not proof that a
  current graph builder can serialize and reload executable implementations.
- The scalar list includes intended HGL concepts; listing `zoned_time` here
  is not evidence of complete native support. Scalar arithmetic and capability
  questions remain where the chapter names them.
- TS-1's timestamp rule describes owned outputs. Sampled inputs, non-peered
  aggregates and keyed withdrawal have their own observations. The latter can
  report removals while invalid; its final contract remains a chapter question.
- The specification's nil for an invalid value or an idle delta is a logical
  observation. The [user guide](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/docs/source/user_guide/concepts/time_series_types.rst)
  describes reading an invalid input as not meaningful. Raw native accessor
  availability and any adapter normalization need explicit evidence.
- Rank-safe reference use is the intended rule. This audit does not establish
  that every runtime path rejects backward references or makes expired stored
  references safe. Those remain separate enforcement/lifetime questions.
- Output writes during start, nested-node error capture, stop failures and
  published effects after evaluation failure need the chapter-specific evidence
  still listed under Points to settle. A simple success trace does not cover them.
- Representation and pair-owner profiles are proposals. Their specified error
  names, layout sizes and borrow rejection are not native API guarantees.

The earlier conflict survey was a dated input to the hgl chapters, not a live
compatibility matrix. This audit supersedes its stale TSD and recursive-field
observations. Update evidence when the source revision changes; do not copy
past validation counts forward as results of the new specification.
