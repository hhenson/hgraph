# Sampled Runtime Contract

This note records the sampled contract that the C++ runtime must match.

## Core rule

Sampled is a view-level semantic. When a `REF`, `TargetLinkState`, or `OutputLinkState` changes identity at tick `t`, every view reached through that rebinding is sampled at `t`.

For a sampled view:

- `modified()` is true at `t` even if the new target did not tick at `t`
- child views inherit sampled semantics from the parent traversal
- container helpers must replay the current bound target instead of trusting a narrow source delta

## Container rule

On a sampled tick:

- `TSB.modified_items()` yields all valid fields
- `TSL.modified_items()` and `modified_values()` yield all valid live children
- `TSD.modified_items()` yields all valid live children
- dict `added_*` / `removed_*` are derived from `transition_snapshot(previous_value)` against the current bound target when that snapshot exists

## Implementation rule

`detail::transition_snapshot(...)` is the source of truth for previous-target snapshots. Output-link and target-link replay must publish current live child values back into container roots. Node-local sampled rescans are fallback behavior and should be removed once the shared layer is correct.

## Reference rule

If Python and C++ disagree on sampled behavior, C++ is wrong unless a requirement explicitly says otherwise.
