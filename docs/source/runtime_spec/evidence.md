# Evidence

Status: source audit, 2026-09-20, hgraph `51b66437af4fa634af2f8f017be721e4ae47a353`.
This source audit read code and tests; it did not run runtime suites.
The later [dynamic-case validation](validation.md) records executed Python/C++
traces, native endpoint checks, accepted variations and their exact scope.

Current C++ rulings and accepted records take precedence over older prose.
These are implementation findings at the pinned revision.

| Subject | Source | Finding |
|---|---|---|
| all_valid | [Static-node tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_static_node.cpp); [slot operations](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/src/hgraph/types/metadata/ts_data_slot_ops.cpp), `tsd_all_valid` | TSD checks its validity and immediate live children's valid; no recursion |
| Membership | Slot operations: `insert_key`, `remove_key`, `record_child_modified` | Membership and value-publication masks are separate; TS-19 names membership |
| Type identity | [Registry tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_type_registry.cpp), storage-category and nominal-identity cases | Identity is independent of storage; portable encoding remains open |
| Activation | [Input tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_ts_input.cpp), activation without notification | Making active does not schedule; sampling is separate |
| Scheduling | [Scheduler tests](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/tests/cpp/test_node_scheduler.cpp) | Startup, tags, cancellation and wall-clock rules differ from internal graph scheduling |
| Recursive fields | [ADR 0012](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/language/docs/design/decisions/0012-recursive-struct-fields.md) | Implemented in both HGL backends for its admitted domain; cross-module struct imports remain pending at this revision |
| State/cache | [ADR 0008](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/language/docs/design/decisions/0008-temporal-contracts-and-target-mappings.md) | C++ admits both; mixed HGL lowering remains pending; native State does not enforce reconstructibility |

## Remaining gaps

- Storable graph descriptions (GRF-1) and `zoned_time` are intended concepts;
  their presence here does not establish native support.
- Sampled inputs, non-peered aggregates and keyed withdrawal have their own
  observations. Withdrawal can report removals while invalid; see TS-15.
- Nil for an invalid value or idle delta is a logical observation. The
  [user guide](https://github.com/hhenson/hgraph/blob/51b66437af4fa634af2f8f017be721e4ae47a353/docs/source/user_guide/concepts/time_series_types.rst) calls invalid input reads meaningless; raw accessor
  behaviour and adapter normalization still need evidence.
- Rank-safe references are intended. Backward-binding enforcement remains
  open. Expired stored references now follow TS-23; both implementations vary
  from the next-cycle expiry confirmed by the user. See dynamic-case validation.
- Layout and pair-owner examples remain proposals, including their error names.

This replaces the earlier conflict survey's stale TSD and recursive-field
findings. Evidence must be refreshed when its source changes.
