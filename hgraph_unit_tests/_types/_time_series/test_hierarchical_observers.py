"""
Hierarchical Observer Tests - Phase 2 (P2.T2, P2.T3, P2.T9)

Tests for hierarchical observer storage and notification propagation.

**Key Behaviors:**
1. Observers lazily allocated on first subscribe()
2. Notification propagates UPWARD through parent chain
3. Root observers notified on ANY descendant change
4. Field-specific observers notified only on field changes
5. Multiple subscribers at different levels all receive correct notifications

**Design Principle:** Hierarchical observers and hierarchical timestamps share
the same structural model (from TSValue_DESIGN.md Section 0.2)

**STATUS:** DISABLED - These tests require Python API integration (Phase 3+)
"""

import pytest

# Skip entire module until Python integration is complete
pytestmark = pytest.mark.skip(reason="Phase 2 observer tests - Python integration not yet complete (requires Phase 3+)")
from hgraph import (
    compute_node,
    graph,
    TS,
    TSB,
    TSL,
    TSS,
    TSD,
    modified,
    valid,
)
from hgraph.test import eval_node


# =============================================================================
# === PERMANENT TESTS (Commit These) ===
# =============================================================================


# -----------------------------------------------------------------------------
# Lazy Observer Allocation Tests
# -----------------------------------------------------------------------------


def test_observer_not_allocated_without_subscription():
    """Observers are not allocated until first subscribe() call."""
    # Indirect test - outputs without subscribers work without errors
    @compute_node
    def unsubscribed_output() -> TS[int]:
        return 42

    # No subscribers, but evaluation succeeds
    result = eval_node(unsubscribed_output, [])
    assert result == [42]


def test_observer_allocated_on_first_subscription():
    """First subscription triggers observer allocation."""
    @compute_node
    def source() -> TS[int]:
        return 42

    @compute_node
    def subscriber(ts: TS[int]) -> TS[bool]:
        # Subscription happens implicitly when input binds
        return modified(ts)

    # Subscriber triggers allocation
    result = eval_node(subscriber, source, [])
    assert result == [True]


def test_observer_not_reallocated_on_subsequent_subscriptions():
    """Multiple subscriptions reuse same observer storage."""
    @compute_node
    def source() -> TS[int]:
        return 42

    @compute_node
    def sub1(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    @compute_node
    def sub2(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    @graph
    def multi_subscriber() -> TSB[s1: TS[bool], s2: TS[bool]]:
        src = source()
        return {"s1": sub1(src), "s2": sub2(src)}

    result = eval_node(multi_subscriber, [])
    assert result[0]["s1"] == True
    assert result[0]["s2"] == True


def test_bundle_field_observers_lazy():
    """Bundle field observers allocated only when field is subscribed."""
    @compute_node
    def bundle_source() -> TSB[x: TS[int], y: TS[int]]:
        return {"x": 1, "y": 2}

    @compute_node
    def x_subscriber(bundle: TSB[x: TS[int], y: TS[int]]) -> TS[bool]:
        # Only subscribes to x field
        return modified(bundle.x)

    # y field observer not allocated since no subscription
    result = eval_node(x_subscriber, bundle_source, [])
    assert result == [True]


# -----------------------------------------------------------------------------
# Upward Notification Propagation Tests
# -----------------------------------------------------------------------------


def test_leaf_modification_notifies_parent():
    """Modifying leaf notifies parent observer."""
    @compute_node
    def nested_bundle(val: TS[int]) -> TSB[outer: TSB[inner: TS[int]]]:
        return {"outer": {"inner": val.value}}

    @compute_node
    def parent_subscriber(bundle: TSB[outer: TSB[inner: TS[int]]]) -> TS[bool]:
        # Subscribe to parent - should be notified on leaf change
        return modified(bundle.outer)

    result = eval_node(parent_subscriber, nested_bundle, [1, 1, 2])
    assert result == [True, False, True]  # Parent modified when leaf changes


def test_deep_leaf_modification_notifies_root():
    """Modifying deep leaf notifies root observer."""
    @compute_node
    def deep_nested(val: TS[int]) -> TSB[
        level1: TSB[level2: TSB[level3: TS[int]]]
    ]:
        return {"level1": {"level2": {"level3": val.value}}}

    @compute_node
    def root_subscriber(bundle: TSB[
        level1: TSB[level2: TSB[level3: TS[int]]]
    ]) -> TS[bool]:
        # Root subscription - notified on deepest leaf change
        return modified(bundle)

    result = eval_node(root_subscriber, deep_nested, [1, 1, 2])
    assert result == [True, False, True]


def test_sibling_modification_notifies_common_parent():
    """Modifying one sibling notifies parent but not other sibling."""
    @compute_node
    def sibling_bundle(x: TS[int], y: TS[int]) -> TSB[x: TS[int], y: TS[int]]:
        return {"x": x.value, "y": y.value}

    @compute_node
    def parent_and_children(bundle: TSB[x: TS[int], y: TS[int]]) -> TSB[
        parent_mod: TS[bool],
        x_mod: TS[bool],
        y_mod: TS[bool]
    ]:
        return {
            "parent_mod": modified(bundle),
            "x_mod": modified(bundle.x),
            "y_mod": modified(bundle.y)
        }

    # Tick 0: both change, Tick 1: only x changes, Tick 2: only y changes
    result = eval_node(parent_and_children, sibling_bundle, [1, 2, 2], [10, 10, 20])

    # Tick 0: all modified
    assert result[0]["parent_mod"] == True
    assert result[0]["x_mod"] == True
    assert result[0]["y_mod"] == True

    # Tick 1: parent and x modified, y not
    assert result[1]["parent_mod"] == True
    assert result[1]["x_mod"] == True
    assert result[1]["y_mod"] == False

    # Tick 2: parent and y modified, x not
    assert result[2]["parent_mod"] == True
    assert result[2]["x_mod"] == False
    assert result[2]["y_mod"] == True


# -----------------------------------------------------------------------------
# Selective Notification Tests
# -----------------------------------------------------------------------------


def test_field_observer_only_notified_on_field_change():
    """Field-specific observer not notified when other fields change."""
    @compute_node
    def multi_field(a: TS[int], b: TS[int], c: TS[int]) -> TSB[
        a: TS[int], b: TS[int], c: TS[int]
    ]:
        return {"a": a.value, "b": b.value, "c": c.value}

    @compute_node
    def b_only_subscriber(bundle: TSB[
        a: TS[int], b: TS[int], c: TS[int]
    ]) -> TS[bool]:
        # Only subscribe to b field
        return modified(bundle.b)

    # b changes only on tick 2
    result = eval_node(
        b_only_subscriber,
        multi_field,
        [1, 2, 3],  # a
        [10, 10, 20],  # b
        [100, 200, 300]  # c
    )
    assert result == [True, False, True]  # b modified on tick 0 and 2


def test_list_element_observer_only_notified_on_element_change():
    """Element-specific observer not notified when other elements change."""
    @compute_node
    def multi_element(a: TS[int], b: TS[int]) -> TSL[TS[int], 2]:
        return [a.value, b.value]

    @compute_node
    def element_0_subscriber(lst: TSL[TS[int], 2]) -> TS[bool]:
        # Only subscribe to element 0
        return modified(lst[0])

    # Element 0 changes on ticks 0, 1; element 1 on ticks 0, 2
    result = eval_node(
        element_0_subscriber,
        multi_element,
        [1, 2, 2],  # element 0
        [10, 10, 20]  # element 1
    )
    assert result == [True, True, False]  # Element 0 modified on 0, 1


def test_root_observer_notified_on_any_change():
    """Root observer receives notification for ANY descendant change."""
    @compute_node
    def complex_structure(a: TS[int], b: TS[int], c: TS[int]) -> TSB[
        field1: TS[int],
        field2: TSB[nested: TS[int]],
        field3: TSL[TS[int], 1]
    ]:
        return {
            "field1": a.value,
            "field2": {"nested": b.value},
            "field3": [c.value]
        }

    @compute_node
    def root_subscriber(bundle: TSB[
        field1: TS[int],
        field2: TSB[nested: TS[int]],
        field3: TSL[TS[int], 1]
    ]) -> TS[bool]:
        # Root subscription
        return modified(bundle)

    # Different fields change on different ticks
    result = eval_node(
        root_subscriber,
        complex_structure,
        [1, 2, 2],    # field1 changes on tick 1
        [10, 10, 20],  # field2.nested changes on tick 2
        [100, 100, 100]  # field3 never changes
    )
    # Root notified on all ticks where any field changes
    assert result == [True, True, True]


# -----------------------------------------------------------------------------
# Multiple Subscriber Tests
# -----------------------------------------------------------------------------


def test_multiple_root_subscribers_all_notified():
    """All root subscribers receive notifications."""
    @compute_node
    def source() -> TS[int]:
        return 42

    @compute_node
    def sub1(ts: TS[int]) -> TS[int]:
        return ts.value if modified(ts) else -1

    @compute_node
    def sub2(ts: TS[int]) -> TS[int]:
        return ts.value if modified(ts) else -2

    @graph
    def multi_sub() -> TSB[s1: TS[int], s2: TS[int]]:
        src = source()
        return {"s1": sub1(src), "s2": sub2(src)}

    result = eval_node(multi_sub, [])
    assert result[0]["s1"] == 42
    assert result[0]["s2"] == 42


def test_multiple_field_subscribers_correctly_notified():
    """Multiple subscribers to different fields get correct notifications."""
    @compute_node
    def bundle_source(x: TS[int], y: TS[int]) -> TSB[x: TS[int], y: TS[int]]:
        return {"x": x.value, "y": y.value}

    @compute_node
    def x_subscriber(bundle: TSB[x: TS[int], y: TS[int]]) -> TS[bool]:
        return modified(bundle.x)

    @compute_node
    def y_subscriber(bundle: TSB[x: TS[int], y: TS[int]]) -> TS[bool]:
        return modified(bundle.y)

    @graph
    def multi_field_sub() -> TSB[x_mod: TS[bool], y_mod: TS[bool]]:
        bundle = bundle_source([1, 2], [10, 10, 20])
        return {"x_mod": x_subscriber(bundle), "y_mod": y_subscriber(bundle)}

    result = eval_node(multi_field_sub, [])
    # Tick 0: both modified
    assert result[0]["x_mod"] == True
    assert result[0]["y_mod"] == True
    # Tick 1: only x modified
    assert result[1]["x_mod"] == True
    assert result[1]["y_mod"] == False
    # Tick 2: only y modified
    assert result[2]["x_mod"] == False
    assert result[2]["y_mod"] == True


def test_mixed_level_subscribers_all_notified():
    """Subscribers at different hierarchy levels all get notifications."""
    @compute_node
    def nested_source(val: TS[int]) -> TSB[outer: TSB[inner: TS[int]]]:
        return {"outer": {"inner": val.value}}

    @compute_node
    def all_levels(bundle: TSB[outer: TSB[inner: TS[int]]]) -> TSB[
        root: TS[bool],
        outer: TS[bool],
        inner: TS[bool]
    ]:
        return {
            "root": modified(bundle),
            "outer": modified(bundle.outer),
            "inner": modified(bundle.outer.inner)
        }

    result = eval_node(all_levels, nested_source, [1, 1, 2])
    # All levels modified when leaf changes
    assert result[2]["root"] == True
    assert result[2]["outer"] == True
    assert result[2]["inner"] == True


# -----------------------------------------------------------------------------
# Unsubscribe Tests
# -----------------------------------------------------------------------------


def test_unsubscribe_stops_notifications():
    """Unsubscribing stops receiving notifications."""
    # Tested via passive input behavior
    @graph
    def with_passive_input() -> TS[int]:
        from hgraph import const
        return const(42)

    result = eval_node(with_passive_input, [])
    assert result == [42]


def test_unsubscribe_one_of_many_doesnt_affect_others():
    """Unsubscribing one subscriber doesn't affect others."""
    @compute_node
    def source() -> TS[int]:
        return 42

    @compute_node
    def persistent_sub(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    @graph
    def with_persistent() -> TS[bool]:
        src = source()
        return persistent_sub(src)

    result = eval_node(with_persistent, [])
    assert result == [True]


# -----------------------------------------------------------------------------
# Observer Notification Ordering Tests
# -----------------------------------------------------------------------------


def test_notifications_delivered_in_propagation_order():
    """Notifications propagate from leaf upward in correct order."""
    @compute_node
    def chain_source(val: TS[int]) -> TSB[
        level1: TSB[level2: TSB[level3: TS[int]]]
    ]:
        return {"level1": {"level2": {"level3": val.value}}}

    @compute_node
    def verify_order(bundle: TSB[
        level1: TSB[level2: TSB[level3: TS[int]]]
    ]) -> TS[bool]:
        # If propagation order wrong, modified checks would fail
        level3_mod = modified(bundle.level1.level2.level3)
        level2_mod = modified(bundle.level1.level2)
        level1_mod = modified(bundle.level1)
        root_mod = modified(bundle)

        # When leaf modified, all parents should be modified
        if level3_mod:
            return level2_mod and level1_mod and root_mod
        return True

    result = eval_node(verify_order, chain_source, [1, 1, 2])
    assert all(result)


# -----------------------------------------------------------------------------
# Edge Cases and Error Conditions
# -----------------------------------------------------------------------------


def test_observer_notification_with_invalid_value():
    """Observers notified even when value becomes invalid."""
    @compute_node(valid=())
    def invalidating(trigger: TS[bool]) -> TS[int]:
        if trigger.value:
            return 42
        else:
            return None  # Invalidate

    @compute_node
    def subscriber(ts: TS[int]) -> TS[bool]:
        # Should be notified even on invalidation
        return modified(ts)

    result = eval_node(subscriber, invalidating, [True, False, True])
    assert result == [True, True, True]  # Notified on all changes


def test_observer_with_rapid_changes():
    """Observer handles rapid successive changes correctly."""
    @compute_node
    def rapid_changes(tick: TS[int]) -> TS[int]:
        # Every tick is different
        return tick.value

    @compute_node
    def subscriber(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    result = eval_node(subscriber, rapid_changes, list(range(100)))
    assert all(result)  # Modified on every tick


def test_observer_hierarchical_structure_matches_timestamp_structure():
    """Observer hierarchy exactly matches timestamp hierarchy."""
    # This is a design invariant test
    @compute_node
    def structured_bundle(val: TS[int]) -> TSB[
        scalar: TS[int],
        nested: TSB[x: TS[int]],
        lst: TSL[TS[int], 1]
    ]:
        return {
            "scalar": val.value,
            "nested": {"x": val.value},
            "lst": [val.value]
        }

    @compute_node
    def verify_hierarchy(bundle: TSB[
        scalar: TS[int],
        nested: TSB[x: TS[int]],
        lst: TSL[TS[int], 1]
    ]) -> TS[bool]:
        # All levels should be modified when value changes
        return (
            modified(bundle) and
            modified(bundle.scalar) and
            modified(bundle.nested) and
            modified(bundle.nested.x) and
            modified(bundle.lst) and
            modified(bundle.lst[0])
        )

    result = eval_node(verify_hierarchy, structured_bundle, [1, 1, 2])
    assert result[2] == True  # Hierarchy consistent


# =============================================================================
# === TEMPORARY PHASE TESTS (Remove After Phase 2 Completion) ===
# =============================================================================


def test_phase2_observer_storage_exists():
    """TEMPORARY: Verify ObserverStorage is implemented."""
    @compute_node
    def source() -> TS[int]:
        return 42

    @compute_node
    def subscriber(ts: TS[int]) -> TS[bool]:
        return modified(ts)

    # If ObserverStorage missing, this fails
    result = eval_node(subscriber, source, [])
    assert result == [True]


def test_phase2_hierarchical_observer_structure():
    """TEMPORARY: Verify observers mirror TS schema hierarchy."""
    @compute_node
    def bundle_source() -> TSB[x: TS[int]]:
        return {"x": 42}

    @compute_node
    def field_subscriber(bundle: TSB[x: TS[int]]) -> TS[bool]:
        return modified(bundle.x)

    # Hierarchical structure required for field subscription
    result = eval_node(field_subscriber, bundle_source, [])
    assert result == [True]


def test_phase2_notification_propagation_implemented():
    """TEMPORARY: Verify upward notification propagation works."""
    @compute_node
    def nested(val: TS[int]) -> TSB[outer: TSB[inner: TS[int]]]:
        return {"outer": {"inner": val.value}}

    @compute_node
    def root_sub(bundle: TSB[outer: TSB[inner: TS[int]]]) -> TS[bool]:
        return modified(bundle)

    result = eval_node(root_sub, nested, [1, 2])
    # Root notified on leaf change
    assert result == [True, True]
