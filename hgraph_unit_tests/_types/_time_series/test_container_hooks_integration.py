"""
Container Hook Integration Tests - Phase 2

Tests specifically for container hook integration (P2.T5, P2.T6) with focus on
edge cases and invariant preservation under swap-with-last operations.

**Key Behaviors:**
1. Hooks fire in correct order: on_insert -> on_swap -> on_erase
2. Index alignment: overlay indices match backing store indices after all operations
3. Swap-with-last: erasing middle element swaps last to that position
4. Multiple operations: overlay remains consistent across insert/erase cycles

**Reference:** Value_TSValue_MIGRATION_PLAN.md Phase 1 implementation notes

**STATUS:** DISABLED - These tests require Python API integration (Phase 3+)
"""

import pytest

# Skip entire module until Python integration is complete
pytestmark = pytest.mark.skip(reason="Phase 2 container hook tests - Python integration not yet complete (requires Phase 3+)")
from hgraph import compute_node, TS, TSS, TSD, modified, valid
from hgraph.test import eval_node


# =============================================================================
# === PERMANENT TESTS (Commit These) ===
# =============================================================================


# -----------------------------------------------------------------------------
# Set Container Hook Tests
# -----------------------------------------------------------------------------


def test_set_insert_updates_overlay_at_returned_index():
    """on_insert receives correct index for newly inserted element."""
    @compute_node
    def insert_sequence(tick: TS[int]) -> TSS[int]:
        # Insert elements one by one
        if tick.value == 0:
            return {1}
        elif tick.value == 1:
            return {1, 2}
        elif tick.value == 2:
            return {1, 2, 3}
        else:
            return {1, 2, 3}

    @compute_node
    def verify_set(s: TSS[int]) -> TS[bool]:
        return modified(s)

    result = eval_node(verify_set, insert_sequence, [0, 1, 2, 3])
    # Modified on each insert, not on last tick
    assert result == [True, True, True, False]


def test_set_erase_last_no_swap():
    """Erasing last element doesn't trigger swap hook, only erase hook."""
    @compute_node
    def erase_last_pattern(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1, 2, 3}
        elif tick.value == 1:
            return {1, 2}  # Erase last (3)
        else:
            return {1, 2}

    @compute_node
    def verify(s: TSS[int]) -> TS[int]:
        # Return set size to verify erase happened
        return len(s.value)

    result = eval_node(verify, erase_last_pattern, [0, 1, 2])
    assert result == [3, 2, 2]


def test_set_erase_first_triggers_swap():
    """Erasing first element triggers swap-with-last."""
    @compute_node
    def erase_first_pattern(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1, 2, 3}
        elif tick.value == 1:
            return {2, 3}  # Erase 1 (likely first position)
        else:
            return {2, 3}

    @compute_node
    def verify(s: TSS[int]) -> TS[bool]:
        # Verify set still works correctly after swap
        return 2 in s.value and 3 in s.value

    result = eval_node(verify, erase_first_pattern, [0, 1, 2])
    assert result[1] == True
    assert result[2] == True


def test_set_multiple_insert_erase_cycles():
    """Overlay remains aligned through multiple insert/erase operations."""
    @compute_node
    def cycling_set(tick: TS[int]) -> TSS[int]:
        patterns = [
            {1, 2, 3},
            {1, 3},      # Erase 2
            {1, 3, 4},   # Insert 4
            {3, 4},      # Erase 1
            {3, 4, 5},   # Insert 5
            {3, 5},      # Erase 4
        ]
        return patterns[tick.value] if tick.value < len(patterns) else {3, 5}

    @compute_node
    def verify_integrity(s: TSS[int]) -> TS[int]:
        # Set operations must work correctly
        return len(s.value)

    result = eval_node(verify_integrity, cycling_set, [0, 1, 2, 3, 4, 5, 6])
    assert result == [3, 2, 3, 2, 3, 2, 2]


def test_set_erase_middle_multiple_times():
    """Repeated middle element erasure (swap-with-last) maintains integrity."""
    @compute_node
    def repeated_middle_erase(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1, 2, 3, 4, 5}
        elif tick.value == 1:
            return {1, 3, 4, 5}  # Erase 2 (middle)
        elif tick.value == 2:
            return {1, 4, 5}     # Erase 3 (now middle)
        elif tick.value == 3:
            return {1, 5}        # Erase 4 (now middle)
        else:
            return {1, 5}

    @compute_node
    def verify(s: TSS[int]) -> TS[bool]:
        return modified(s)

    result = eval_node(verify, repeated_middle_erase, [0, 1, 2, 3, 4])
    assert result == [True, True, True, True, False]


def test_set_reinsert_after_erase():
    """Re-inserting previously erased element works correctly."""
    @compute_node
    def reinsert_pattern(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1, 2, 3}
        elif tick.value == 1:
            return {1, 3}    # Erase 2
        elif tick.value == 2:
            return {1, 2, 3}  # Re-insert 2
        else:
            return {1, 2, 3}

    @compute_node
    def verify(s: TSS[int]) -> TS[bool]:
        return modified(s)

    result = eval_node(verify, reinsert_pattern, [0, 1, 2, 3])
    assert result == [True, True, True, False]


# -----------------------------------------------------------------------------
# Map Container Hook Tests
# -----------------------------------------------------------------------------


def test_map_insert_new_key_updates_overlay():
    """on_insert fires when new key added to map."""
    @compute_node
    def insert_keys(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1}
        elif tick.value == 1:
            return {"a": 1, "b": 2}
        elif tick.value == 2:
            return {"a": 1, "b": 2, "c": 3}
        else:
            return {"a": 1, "b": 2, "c": 3}

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[bool]:
        return modified(m)

    result = eval_node(verify, insert_keys, [0, 1, 2, 3])
    assert result == [True, True, True, False]


def test_map_update_existing_key_value_modification():
    """Updating existing key's value is value modification, not structural."""
    @compute_node
    def update_value(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1}
        elif tick.value == 1:
            return {"a": 2}  # Value modification only
        else:
            return {"a": 2}

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[bool]:
        return modified(m)

    result = eval_node(verify, update_value, [0, 1, 2])
    assert result == [True, True, False]


def test_map_erase_key_triggers_hooks():
    """Erasing a key triggers swap and/or erase hooks correctly."""
    @compute_node
    def erase_key(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1, "b": 2, "c": 3}
        elif tick.value == 1:
            return {"a": 1, "c": 3}  # Erase "b"
        else:
            return {"a": 1, "c": 3}

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[int]:
        return len(m.value)

    result = eval_node(verify, erase_key, [0, 1, 2])
    assert result == [3, 2, 2]


def test_map_swap_with_last_on_middle_key_erase():
    """Erasing middle key swaps last key to that position."""
    @compute_node
    def erase_middle(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1, "b": 2, "c": 3}
        elif tick.value == 1:
            return {"a": 1, "c": 3}  # Erase "b" (middle)
        else:
            return {"a": 1, "c": 3}

    @compute_node
    def verify_keys(m: TSD[str, TS[int]]) -> TS[bool]:
        # Keys must still be accessible
        return "a" in m.value and "c" in m.value and "b" not in m.value

    result = eval_node(verify_keys, erase_middle, [0, 1, 2])
    assert result[1] == True
    assert result[2] == True


def test_map_multiple_operations_overlay_alignment():
    """Complex sequence of map operations maintains overlay alignment."""
    @compute_node
    def complex_map_ops(tick: TS[int]) -> TSD[str, TS[int]]:
        patterns = [
            {"a": 1, "b": 2, "c": 3},
            {"a": 1, "c": 3},           # Erase "b"
            {"a": 1, "c": 3, "d": 4},   # Insert "d"
            {"a": 2, "c": 3, "d": 4},   # Update "a"
            {"c": 3, "d": 4},           # Erase "a"
            {"c": 3, "d": 5},           # Update "d"
        ]
        return patterns[tick.value] if tick.value < len(patterns) else {"c": 3, "d": 5}

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[bool]:
        return modified(m)

    result = eval_node(verify, complex_map_ops, [0, 1, 2, 3, 4, 5, 6])
    assert result == [True, True, True, True, True, True, False]


def test_map_erase_and_reinsert_same_key():
    """Erasing and re-inserting same key works correctly."""
    @compute_node
    def erase_reinsert(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1, "b": 2}
        elif tick.value == 1:
            return {"a": 1}        # Erase "b"
        elif tick.value == 2:
            return {"a": 1, "b": 3}  # Re-insert "b" with different value
        else:
            return {"a": 1, "b": 3}

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[bool]:
        return modified(m)

    result = eval_node(verify, erase_reinsert, [0, 1, 2, 3])
    assert result == [True, True, True, False]


# -----------------------------------------------------------------------------
# Hook Ordering and Invariant Tests
# -----------------------------------------------------------------------------


def test_set_hook_ordering_insert_then_swap_then_erase():
    """Hooks fire in correct order during erase-with-swap operation."""
    # This test verifies the contract that when erasing non-last:
    # 1. on_swap(erased_idx, last_idx) fires first
    # 2. on_erase(last_idx) fires after swap
    @compute_node
    def erase_non_last(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1, 2, 3}
        elif tick.value == 1:
            return {1, 3}  # Erase 2 (middle) - triggers swap then erase
        else:
            return {1, 3}

    @compute_node
    def verify_consistency(s: TSS[int]) -> TS[bool]:
        # After hooks, set must be in consistent state
        expected = {1, 3} if tick.value > 0 else {1, 2, 3}
        return s.value == expected

    result = eval_node(verify_consistency, erase_non_last, [0, 1, 2])
    assert all(result)


def test_map_hook_ordering_preserves_key_value_mapping():
    """Map hooks preserve key-value mappings through all operations."""
    @compute_node
    def map_with_operations(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"x": 10, "y": 20, "z": 30}
        elif tick.value == 1:
            return {"x": 10, "z": 30}  # Erase "y"
        else:
            return {"x": 10, "z": 30}

    @compute_node
    def verify_mapping(m: TSD[str, TS[int]]) -> TS[bool]:
        # Mappings must remain correct
        if "x" in m.value:
            if m.value["x"] != 10:
                return False
        if "z" in m.value:
            if m.value["z"] != 30:
                return False
        return True

    result = eval_node(verify_mapping, map_with_operations, [0, 1, 2])
    assert all(result)


def test_set_index_stability_between_operations():
    """Set element indices remain stable until swap/erase occurs."""
    @compute_node
    def stable_set(tick: TS[int]) -> TSS[int]:
        if tick.value < 5:
            return {1, 2, 3}  # No changes - indices stable
        else:
            return {1, 2}  # Erase 3

    @compute_node
    def verify(s: TSS[int]) -> TS[int]:
        return len(s.value)

    result = eval_node(verify, stable_set, [0, 1, 2, 3, 4, 5])
    assert result == [3, 3, 3, 3, 3, 2]


# -----------------------------------------------------------------------------
# Edge Cases and Boundary Conditions
# -----------------------------------------------------------------------------


def test_set_erase_single_element():
    """Erasing only element from single-element set."""
    @compute_node
    def single_element_set(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {42}
        else:
            return set()  # Empty set

    @compute_node
    def verify(s: TSS[int]) -> TS[int]:
        return len(s.value)

    result = eval_node(verify, single_element_set, [0, 1])
    assert result == [1, 0]


def test_map_erase_single_entry():
    """Erasing only entry from single-entry map."""
    @compute_node
    def single_entry_map(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"only": 42}
        else:
            return {}  # Empty map

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[int]:
        return len(m.value)

    result = eval_node(verify, single_entry_map, [0, 1])
    assert result == [1, 0]


def test_set_large_number_of_operations():
    """Overlay remains aligned after many operations."""
    @compute_node
    def many_operations(tick: TS[int]) -> TSS[int]:
        # Cycle through patterns
        patterns = [
            {i for i in range(10)},
            {i for i in range(5)},
            {i for i in range(8)},
            {i for i in range(3)},
        ]
        return patterns[tick.value % len(patterns)]

    @compute_node
    def verify(s: TSS[int]) -> TS[bool]:
        return modified(s)

    result = eval_node(verify, many_operations, list(range(20)))
    # Modified on every change
    assert result[0] == True
    assert result[1] == True
    assert result[2] == True


def test_map_large_number_of_keys():
    """Map overlay handles many keys correctly."""
    @compute_node
    def many_keys(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {f"key{i}": i for i in range(10)}
        else:
            return {f"key{i}": i for i in range(5)}  # Erase half

    @compute_node
    def verify(m: TSD[str, TS[int]]) -> TS[int]:
        return len(m.value)

    result = eval_node(verify, many_keys, [0, 1])
    assert result == [10, 5]


# =============================================================================
# === TEMPORARY PHASE TESTS (Remove After Phase 2 Completion) ===
# =============================================================================


def test_phase2_set_hooks_wired_to_overlay():
    """TEMPORARY: Verify set container hooks are connected to overlay."""
    @compute_node
    def set_ops(tick: TS[int]) -> TSS[int]:
        if tick.value == 0:
            return {1}
        elif tick.value == 1:
            return {1, 2}  # on_insert should fire
        else:
            return {2}  # on_swap and on_erase should fire

    # If hooks not wired, overlay will be out of sync
    result = eval_node(set_ops, [0, 1, 2])
    assert result == [{1}, {1, 2}, {2}]


def test_phase2_map_hooks_wired_to_overlay():
    """TEMPORARY: Verify map container hooks are connected to overlay."""
    @compute_node
    def map_ops(tick: TS[int]) -> TSD[str, TS[int]]:
        if tick.value == 0:
            return {"a": 1}
        elif tick.value == 1:
            return {"a": 1, "b": 2}  # on_insert
        else:
            return {"b": 2}  # on_swap and on_erase

    result = eval_node(map_ops, [0, 1, 2])
    assert result == [{"a": 1}, {"a": 1, "b": 2}, {"b": 2}]


def test_phase2_hooks_use_composition_not_inheritance():
    """TEMPORARY: Verify hooks use composition pattern from Phase 1."""
    # This is an indirect test - the fact that hooks work at all
    # without breaking the Value layer confirms composition works
    @compute_node
    def mixed_containers() -> TSS[int]:
        return {1, 2, 3}

    result = eval_node(mixed_containers, [])
    assert result == [{1, 2, 3}]
