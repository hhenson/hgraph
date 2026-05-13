# Design Note: v2 Keyed Slot Store for `map_` and `TSD`

Date: 2026-04-17  
Status: Draft design note  
Scope: Define the reusable slot-indexed payload store used by `map_` and, later, `TSD`.

## 1. Correction

The keyed slot store must not re-implement key tracking.

In this codebase, keyed lifecycle already belongs to the `TSS` / associative-value protocol:

1. key membership
2. stable slot identity
3. added / removed / updated slot tracking
4. retained removed payload visibility until the mutation epoch releases it

That protocol is already the source of truth for keyed structures. `map_` is driven by a `TSS` of keys and must integrate into that protocol in the same way that `TSD` value storage does. The keyed slot store is therefore not a second keyed container. It is only the payload store behind the existing keyed protocol.

## 2. Problem

`map_` needs per-key retained runtime state with these semantics:

1. payload is addressed by stable slot id
2. when a key is removed, the child graph is stopped but retained
3. when the slot is finally erased by the keyed protocol, the child graph is disposed and destroyed
4. storage should be slot-oriented and in-place

The current legacy `tsd_map_node` achieves similar behavior with ad hoc `key -> graph` maps. That proves the lifecycle, but not the right architecture.

## 3. Design Goal

Extract one reusable slot-indexed payload store that:

1. is driven by an external keyed protocol
2. owns in-place payload lifetime by slot id
3. does not own `key -> slot`
4. does not own added / removed tracking
5. can be used by both `map_` and `TSD`

The external keyed protocol for both cases is the existing `TSS` / associative-value layer.

The specific behavior to preserve is the current double indexing used by delta map values:

1. slot id -> payload pointer table
2. payload pointer -> chained stable storage block

That is the mechanism that keeps payload addresses stable across growth. This is the first behavior that should be extracted and reused.

## 4. Core Model

The model should be:

```text
TSS / associative delta protocol
  owns key membership and slot identity

KeyedSlotStore<T>
  owns payload lifetime for those slots

map_
  stores per-slot nested child runtime payloads

TSD
  stores per-slot child TS payloads
```

This means:

1. `TSS` tells us which slot was added, removed, updated, or finally released
2. `KeyedSlotStore<T>` answers only: "is there a payload constructed at slot N, and if so manage its lifetime"

## 5. What The Store Owns

`KeyedSlotStore<T>` should own:

1. slot-indexed payload pointers
2. chained stable storage blocks
3. aligned raw storage per slot
4. construction state per slot
5. placement construction and destruction of `T`
6. capacity management aligned to slot capacity

It should not own:

1. key membership
2. key hashing
3. key lookup
4. removed/live semantics as a parallel state machine
5. delta tracking

The removed/live distinction comes from the `TSS` / map delta protocol, not from the store.

## 6. Utility API

Suggested public shape:

```cpp
namespace hgraph
{
    template <typename T>
    class KeyedSlotStore
    {
    public:
        using slot_id = size_t;

        void reserve_slots(size_t slot_capacity);
        void clear();

        [[nodiscard]] size_t slot_capacity() const noexcept;
        [[nodiscard]] bool has_value(slot_id slot) const noexcept;

        [[nodiscard]] T &value(slot_id slot) noexcept;
        [[nodiscard]] const T &value(slot_id slot) const noexcept;
        [[nodiscard]] T *try_value(slot_id slot) noexcept;
        [[nodiscard]] const T *try_value(slot_id slot) const noexcept;

        template <typename... Args>
        T &emplace_at(slot_id slot, Args &&...args);

        void destroy_at(slot_id slot) noexcept;
    };
}
```

Important points:

1. the store is slot-based, not key-based
2. `reserve_slots()` follows the slot capacity of the keyed protocol
3. `emplace_at(slot)` is called when a new keyed slot becomes active
4. `destroy_at(slot)` is called only when the keyed protocol says the retained slot payload can be physically released

## 7. Store Invariants

`KeyedSlotStore<T>` should enforce:

1. slot ids are supplied externally
2. a payload at a slot is either constructed or not
3. `emplace_at(slot)` requires the slot not already be constructed
4. `destroy_at(slot)` is a no-op or checked failure if not constructed
5. payload address remains stable while constructed
6. resizing never moves a constructed payload without proper move construction support

Implementation-wise, the simplest shape is:

1. slot pointer table indexed by stable slot id
2. appended aligned blocks that never move existing payloads
3. one construction bit per slot
4. explicit placement new / destruction

## 8. Relationship to TSS / Associative Protocol

This is the critical integration rule:

1. `TSS` and `MapDeltaView` remain the only authority on slot identity and removed-slot retention

The driving flow should look like:

1. inspect keyed delta
2. for each added slot:
   create payload at that slot
3. for each removed slot:
   stop / detach payload at that slot, but do not destroy it
4. when the keyed layer finally releases the removed payload:
   destroy the payload at that slot

The slot store does not decide when a slot is removed or erased. It only responds.

## 9. `map_` Payload Type

For `map_`, the slot payload should be the per-key nested runtime state.

Suggested initial payload:

```cpp
namespace hgraph
{
    struct MapSlotValue
    {
        ChildGraphInstance child;
        engine_time_t next_scheduled{MAX_DT};
        bool bound{false};
        bool scheduled{false};
    };
}
```

Notes:

1. `ChildGraphInstance` is already the correct graph-local lifecycle unit
2. `map_` should not split that lifecycle across separate side maps
3. the payload should contain only state that is truly slot-local

## 10. `map_` Contract

`map_` should be driven by the key `TSS`.

### 10.1 Add

When the key protocol reports an added slot:

1. obtain the slot id from the keyed delta
2. emplace `MapSlotValue` at that slot
3. initialise the child graph instance
4. bind the keyed child inputs
5. start the child graph
6. schedule it if required

### 10.2 Remove

When the key protocol reports a removed slot:

1. obtain the slot id from the keyed delta
2. unbind the child if required
3. stop the child graph
4. retain the payload in the slot store

This is logical removal only.

### 10.3 Erase

When the keyed layer finally releases that retained removed slot:

1. locate the same slot id
2. dispose the child graph
3. destroy the slot payload with `destroy_at(slot)`

This is the physical cleanup step.

## 11. `TSD` Contract

The same utility should later back `TSD` value payload storage.

That means:

1. `TSD` continues to use its `key_set()` and associative delta protocol for key tracking
2. the slot store manages the payload objects stored at those slots
3. `TSD` and `map_` differ only in payload type and lifecycle hooks

This is the real reuse target:

1. one keyed protocol
2. one slot payload store
3. multiple payload types

## 12. Why This Is Better

This gives the right separation:

1. keyed semantics stay where they already belong
2. slot payload lifetime becomes reusable
3. `map_` stops inventing its own keyed container
4. `TSD` and `map_` can converge on the same slot-storage substrate

Most importantly, it avoids the design error of building a second key registry beside `TSS`.

## 13. Recommended First Extraction

The first extraction should be:

1. add `KeyedSlotStore<T>` as a slot-indexed payload utility
2. keep it fully graph-agnostic and key-agnostic
3. add focused unit tests for:
   - reserve and slot-capacity growth
   - in-place construct at slot
   - stop-free retain semantics at the caller layer
   - explicit destroy at slot
   - stable address while payload remains constructed

Only after that should `map_` be migrated.

## 14. Suggested Minimal Test Matrix

Generic store tests:

1. construct values at sparse slots
2. verify `has_value(slot)` and `try_value(slot)`
3. destroy one slot without affecting neighbors
4. reserve larger capacity and verify existing payloads remain valid
5. clear destroys all constructed payloads

`map_` integration tests:

1. added key slot constructs child payload
2. removed key slot stops child but retains payload
3. final slot release disposes and destroys payload
4. reused keyed slot constructs a fresh child payload
5. output slot and child-runtime slot stay aligned

## 15. Non-Goals

This utility should not:

1. implement `TSS`
2. implement `TSD`
3. perform key lookup
4. expose added / removed key queries
5. know about boundary binding
6. know about Python delta formatting

Those concerns already exist elsewhere and should stay there.

## 16. Decision Summary

The intended architecture is:

1. keyed membership and slot tracking stay in the existing `TSS` / associative protocol
2. `KeyedSlotStore<T>` is only the slot-indexed payload store
3. `map_` uses `KeyedSlotStore<MapSlotValue>`
4. `TSD` should later use the same utility for its per-slot child payloads
5. remove means stop and retain because the keyed protocol still retains the slot
6. erase means dispose and destroy when the keyed protocol finally releases the slot

That is the substrate we should build before migrating `map_`.
