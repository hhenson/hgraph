# RFC: TSInput Active Path Tracking and Output Boundary Attachments

Date: 2026-03-23  
Status: Draft for focused implementation  
Scope: Define how `TSInput`, `TSOutput`, and `TSValue` should cooperate to retain active subscription intent across dynamic structure changes and REF bind/unbind boundaries.

## 1. Context

This RFC narrows in on one part of the TS runtime design:
1. How active subscription intent should be represented.
2. How that intent should survive dynamic collection changes.
3. How REF and target-link bind/unbind should rebuild live subscriptions correctly.

It updates the active-state part of `docs/design/ts_value/design/05_TSOUTPUT_TSINPUT.md`.

Current relevant code:
1. Shared TS storage/value layer: `cpp/include/hgraph/types/time_series/ts_value.h`
2. Current input endpoint shape with mirrored active state: `cpp/include/hgraph/types/time_series/ts_input.h`
3. Current input-view behavior and collection activation gaps: `cpp/src/cpp/types/time_series/ts_input_view.cpp`
4. Target-link boundary state and scheduling notifier: `cpp/include/hgraph/types/time_series/time_series_state.h`
5. Output endpoint and alternative storage: `cpp/include/hgraph/types/time_series/ts_output.h`

## 2. Goals

1. Keep `TSInput` and `TSOutput` as the only owning root endpoint objects.
2. Keep `TSValue` as the shared nested storage and dispatcher-backed structure mechanism.
3. Remove the need for a mirrored input-side active-state value tree.
4. Preserve active subscription intent for deep logical paths even when those paths disappear and later reappear.
5. Allow output-side REF and target-link bind/unbind to rebuild live subscriptions correctly.
6. Keep the active-tracking model sparse and cheap for the common case where deep subscriptions are uncommon.

## 3. Non-Goals

1. Finalize the exact container implementation for the sparse active-path registry.
2. Finalize the exact public builder API for `TSInputBuilder` / `TSOutputBuilder`.
3. Rework the broader `TSValue` storage layout in this RFC.
4. Reintroduce child `TSInput` / `TSOutput` owning objects below the root.

## 4. Agreed Runtime Ownership Model

## 4.1 Root containers vs nested structure

The agreed runtime split is:
1. `TSInput` and `TSOutput` are the only owning root endpoint containers.
2. `TSValue` remains the shared nested storage mechanism used underneath them.
3. Descendants are represented by dispatcher/view navigation, not by allocating child endpoint objects.

This keeps the current intended role of `TSValue` intact while avoiding endpoint-specific state inside every nested child.

## 4.2 Peer vs non-peer is not endpoint-specific

Peer vs non-peer behavior must not be treated as "input-only" state.

Outputs can also cross peer/non-peer boundaries as a consequence of `REF` handling and alternative representations. That means:
1. Peer/non-peer must be represented in dispatch/context resolution.
2. The endpoint type alone (`TSInput` vs `TSOutput`) is not enough to determine boundary behavior.

## 5. Problem Statement

The current mirrored active-state approach on `TSInput` does not scale well:
1. It duplicates structure that is primarily owned by the bound output side.
2. It becomes awkward when dynamic descendants disappear and later reappear.
3. It does not match the real ownership split for REF bind/unbind.

Also, active state cannot be inferred from current subscriber membership alone.

Subscriber membership answers "currently connected". It does not answer "should this logical path be active whenever it exists?".

Those are different concepts:
1. Desired active state: persistent intent owned by the input.
2. Live subscriptions: currently realized connections against the present bound/output structure.

## 6. Motivating Example

Consider:

```text
ts : TSD[str, TSD[int, TS[bool]]]
```

Suppose the input activates:

```text
ts["k1"][1]
```

If key `1` disappears under `"k1"` and later reappears, the runtime must:
1. Remove the live leaf subscription when `1` disappears.
2. Retain the fact that `ts["k1"][1]` is still desired-active.
3. Recreate the live leaf subscription automatically when `1` comes back.

This is the core requirement that rules out using current subscriber membership as the source of truth for `active()`.

## 7. Proposed Model

## 7.1 Canonical desired-active state is input-owned

`TSInput` should own the canonical desired-active state for logical paths.

That state should be represented as a sparse active-path trie, or an equivalent sparse path registry, rather than a mirrored schema-shaped value tree.

Important properties:
1. Only active paths are stored.
2. Deep subscriptions remain cheap in the common case because inactive structure is not represented.
3. The registry stores logical path segments, not bound-output object identities.

Typical path segments include:
1. Bundle field name or field index.
2. List index.
3. Dict key.
4. Other collection element identities later, as needed.

This input-owned structure is the source of truth for semantic active state.

## 7.2 Live subscription realization is output-owned while bound

While a branch is bound through a target-link or REF boundary, the output side should own the live realization of the relevant desired-active subtree.

The ownership split is:
1. Input owns intent.
2. Output owns realization while bound.

The output should not take ownership of the canonical trie itself. Instead it should hold a stable attachment, projection, or lease for the relevant subtree so it can:
1. React to dynamic structure changes.
2. Rebuild subscriptions when keys/elements reappear.
3. Handle REF bind/unbind correctly.

## 7.3 Boundary-keyed output attachments

The output-side registry must be keyed by binding boundary, not only by `TSInput*`.

Practical boundary keys:
1. Direct bind boundary: `TSInput*`
2. REF / target-link boundary: `TargetLinkState*`

This matters because the same input can cross multiple REF/link boundaries and may bind to the same output in more than one place.

Using only `TSInput*` as the registry key is too coarse unless the value contains an additional boundary-level attachment map.

## 7.4 Scheduling target and boundary marker

The design should keep scheduling information small and explicit:
1. At the root boundary, scheduling can route through `TSInput` itself.
2. Below a target-link boundary, scheduling should route through `TargetLinkState::scheduling_notifier`.

This means `TSInput*` is a valid default `Notifiable` target, but it is not sufficient to represent the full active-state model by itself.

The tracked context still needs to know which scheduling/binding boundary currently applies.

## 7.5 Structural watchers and replay

For each active subtree that is currently attached to an output boundary, the output-side realization should:
1. Install structural watchers on dynamic ancestors relevant to active descendants.
2. Remove live subscriptions below a point when the corresponding child disappears.
3. Keep the canonical desired-active subtree intact on the input.
4. Replay the relevant desired-active subtree when keys/elements reappear.
5. On REF or target-link unbind, drop the live realization for that boundary.
6. On rebind, rebuild the live realization for that boundary from the canonical input-owned desired-active subtree.

## 8. Conceptual Data Model

The exact implementation can vary, but the conceptual shape is:

```cpp
struct ActiveSubtreeHandle {
    // Stable id or arena-backed pointer into the input-owned active-path registry.
};

using BindingOwner = std::variant<TSInput*, TargetLinkState*>;

struct OutputBoundaryAttachment {
    BindingOwner owner;
    TSInput* input;
    ActiveSubtreeHandle subtree;
    Notifiable* scheduler_target;
    // Output-owned live subscription realization/cache for this boundary.
};
```

Important constraint:
1. Do not store raw references/iterators into trie containers if normal insert/erase operations can invalidate them.
2. Use stable node ids, arena-backed nodes, or another representation with explicit stability guarantees.

## 9. Worked Example

For:

```text
ts : TSD[str, TSD[int, TS[bool]]]
```

Assume the input marks `ts["k1"][1]` active.

Desired-active side:
1. `TSInput` stores the sparse path `["k1", 1]` in its canonical active-path registry.

Live realization side:
1. The relevant output boundary attachment watches the outer `TSD` branch for `"k1"`.
2. If `"k1"` exists, it watches the inner `TSD` branch for key `1`.
3. If `1` exists, it realizes the live leaf subscription.
4. If `1` disappears, only the live leaf subscription is removed.
5. The canonical desired-active path `["k1", 1]` remains unchanged on the input.
6. If `1` reappears later, the output-side attachment replays that subtree and recreates the leaf subscription.

The same principle applies when a REF/link boundary unbinds and later rebinds:
1. Unbind removes the live realization for that boundary.
2. The canonical desired-active subtree remains owned by the input.
3. Rebind recreates the output-side attachment and replays the desired-active subtree against the newly bound target.

## 10. Consequences for TSValue / TSInput / TSOutput

1. `TSValue` remains the shared nested storage and navigation substrate.
2. `TSInput` remains the owner of canonical active intent.
3. `TSOutput` becomes the natural owner of boundary-local live subscription realization because it owns the dynamic structure and REF bind/unbind behavior at that level.
4. The current mirrored active-state value on `TSInput` should be removed or replaced by the sparse desired-active registry.
5. `TSInputViewContext` should carry boundary-aware scheduling information rather than treating subscriber membership as active-state truth.

## 11. Builder and Dispatcher Implications

This RFC does not fully specify the builder API, but it does constrain the shape:
1. Endpoint-specific builders/factories should still create root `TSInput` / `TSOutput` objects.
2. `TSValue` remains the nested dispatch/storage layer beneath them.
3. Dispatch/context resolution must be able to cross peer/non-peer boundaries on both input and output paths.
4. Input-side active operations need access to the canonical active-path registry.
5. Output-side bind/unbind logic needs access to boundary-local attachments that reference input-owned active subtrees.

## 12. Implementation Notes

Recommended implementation order:
1. Replace the mirrored `TSInput` active-state storage with a sparse canonical active-path registry.
2. Define stable subtree handles for registry nodes.
3. Add output-side boundary attachment storage keyed by binding boundary.
4. Route REF/target-link unbind and rebind through attachment teardown/replay.
5. Update input/output dispatch and view-context code to use the new split.

## 13. Open Questions

1. Exact path-segment representation for sets and other identity-based collections.
2. Exact output attachment lifetime API and ownership mechanics.
3. Whether the boundary attachment cache should live directly on `TSOutput` or on a boundary-specific helper owned by the output-side runtime state.
4. The most practical stable-handle implementation for the active-path registry.

## 14. Summary

The agreed design is:
1. `TSInput` and `TSOutput` remain the only owning root endpoint objects.
2. `TSValue` remains the shared nested storage mechanism.
3. Canonical active intent is stored sparsely on the input as logical paths.
4. Live subscription realization is owned by the output while the relevant branch is bound.
5. Output-side realization is keyed by binding boundary so it can survive REF and dynamic collection changes without duplicating the full structure on the input.
