"""Tests for TrackedSetStorage and SetDeltaValue."""

import pytest

# Import from _hgraph module
try:
    from hgraph._hgraph import (
        TypeRegistry,
        TrackedSetStorage,
        TrackedSetView,
        SetDeltaValue,
        PlainValue,
        value,
    )
except ImportError:
    pytest.skip("TrackedSet types not available in bindings", allow_module_level=True)


@pytest.fixture
def int_set_schema():
    """Get schema for set of integers."""
    int_type = value.scalar_type_meta_int64()
    return TypeRegistry.instance().set(int_type).build()


@pytest.fixture
def int_element_type():
    """Get element type for integers."""
    return value.scalar_type_meta_int64()


class TestTrackedSetStorage:
    """Tests for TrackedSetStorage."""

    def test_construction(self, int_element_type):
        """Test TrackedSetStorage construction."""
        storage = TrackedSetStorage(int_element_type)
        assert storage.size() == 0
        assert storage.empty()
        assert not storage.has_delta()

    def test_add_element(self, int_element_type):
        """Test adding elements with delta tracking."""
        storage = TrackedSetStorage(int_element_type)

        # Add element
        elem = PlainValue(int_element_type)
        elem.from_python(42)
        assert storage.add(elem.view())

        # Verify state
        assert storage.size() == 1
        assert storage.contains(elem.view())
        assert storage.was_added(elem.view())
        assert not storage.was_removed(elem.view())
        assert storage.has_delta()

    def test_add_duplicate_returns_false(self, int_element_type):
        """Test adding duplicate element returns false."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        # First add succeeds
        assert storage.add(elem.view())
        # Second add fails (already present)
        assert not storage.add(elem.view())

    def test_remove_element(self, int_element_type):
        """Test removing elements with delta tracking."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        # Add and clear deltas
        storage.add(elem.view())
        storage.clear_deltas()

        # Now remove
        assert storage.remove(elem.view())

        # Verify state
        assert storage.size() == 0
        assert not storage.contains(elem.view())
        assert not storage.was_added(elem.view())
        assert storage.was_removed(elem.view())

    def test_remove_nonexistent_returns_false(self, int_element_type):
        """Test removing nonexistent element returns false."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        assert not storage.remove(elem.view())

    def test_add_after_remove_same_cycle(self, int_element_type):
        """Test add after remove in same cycle (un-removes)."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        # Add, clear deltas, then remove
        storage.add(elem.view())
        storage.clear_deltas()
        storage.remove(elem.view())

        # Now add again in same cycle - should un-remove
        assert storage.add(elem.view())
        assert storage.contains(elem.view())
        assert not storage.was_removed(elem.view())
        # Should NOT be in added since it was already there before cycle
        assert not storage.was_added(elem.view())

    def test_remove_after_add_same_cycle(self, int_element_type):
        """Test remove after add in same cycle (un-adds)."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        # Add then remove in same cycle
        storage.add(elem.view())
        storage.remove(elem.view())

        # Should be gone and not in any delta
        assert not storage.contains(elem.view())
        assert not storage.was_added(elem.view())
        assert not storage.was_removed(elem.view())

    def test_clear_deltas(self, int_element_type):
        """Test clear_deltas removes delta tracking."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)

        storage.add(elem.view())
        assert storage.has_delta()

        storage.clear_deltas()
        assert not storage.has_delta()
        # Value should still be there
        assert storage.contains(elem.view())

    def test_clear_all(self, int_element_type):
        """Test clear removes all elements and tracks as removed."""
        storage = TrackedSetStorage(int_element_type)

        # Add elements
        for i in range(3):
            elem = PlainValue(int_element_type)
            elem.from_python(i)
            storage.add(elem.view())

        storage.clear_deltas()

        # Clear
        storage.clear()

        # All should be removed
        assert storage.empty()
        # Check one element was tracked as removed
        elem = PlainValue(int_element_type)
        elem.from_python(0)
        assert storage.was_removed(elem.view())

    def test_iteration(self, int_element_type):
        """Test iteration over set elements."""
        storage = TrackedSetStorage(int_element_type)

        # Add elements
        for i in [1, 2, 3]:
            elem = PlainValue(int_element_type)
            elem.from_python(i)
            storage.add(elem.view())

        # Iterate and collect values
        values = []
        for elem in storage.value():
            values.append(elem.as_int())

        assert sorted(values) == [1, 2, 3]


class TestTrackedSetView:
    """Tests for TrackedSetView."""

    def test_view_read_access(self, int_element_type):
        """Test TrackedSetView provides read access."""
        storage = TrackedSetStorage(int_element_type)

        elem = PlainValue(int_element_type)
        elem.from_python(42)
        storage.add(elem.view())

        view = TrackedSetView(storage)
        assert view.size() == 1
        assert view.contains(elem.view())
        assert view.was_added(elem.view())

    def test_mutable_view(self, int_element_type):
        """Test TrackedSetView provides mutation."""
        storage = TrackedSetStorage(int_element_type)
        view = TrackedSetView(storage)

        elem = PlainValue(int_element_type)
        elem.from_python(42)
        view.add(elem.view())

        assert storage.size() == 1
        assert storage.contains(elem.view())

    def test_view_iteration(self, int_element_type):
        """Test iteration through view."""
        storage = TrackedSetStorage(int_element_type)

        for i in [1, 2]:
            elem = PlainValue(int_element_type)
            elem.from_python(i)
            storage.add(elem.view())

        view = TrackedSetView(storage)
        values = [elem.as_int() for elem in view]
        assert sorted(values) == [1, 2]


class TestSetDeltaValue:
    """Tests for SetDeltaValue."""

    def test_construction(self, int_element_type):
        """Test SetDeltaValue construction."""
        delta = SetDeltaValue(int_element_type)
        assert delta.empty()
        assert delta.added_count() == 0
        assert delta.removed_count() == 0

    def test_from_views(self, int_element_type, int_set_schema):
        """Test SetDeltaValue construction from views."""
        # Create sets for added and removed
        added_set = PlainValue(int_set_schema)

        added_set.emplace()
        removed_set = PlainValue(int_set_schema)

        removed_set.emplace()
        # Add elements
        elem1 = PlainValue(int_element_type)
        elem1.from_python(1)
        elem2 = PlainValue(int_element_type)
        elem2.from_python(2)

        added_set.view().as_set().add(elem1.view())
        removed_set.view().as_set().add(elem2.view())

        # Create delta
        delta = SetDeltaValue(
            added_set.view().as_set(),
            removed_set.view().as_set(),
            int_element_type,
        )

        assert delta.added_count() == 1
        assert delta.removed_count() == 1

    def test_to_python(self, int_element_type, int_set_schema):
        """Test SetDeltaValue to_python conversion."""
        # Create sets
        added_set = PlainValue(int_set_schema)

        added_set.emplace()
        elem = PlainValue(int_element_type)
        elem.from_python(42)
        added_set.view().as_set().add(elem.view())

        removed_set = PlainValue(int_set_schema)

        removed_set.emplace()
        # Create delta
        delta = SetDeltaValue(
            added_set.view().as_set(),
            removed_set.view().as_set(),
            int_element_type,
        )

        py_delta = delta.to_python()
        assert "added" in py_delta
        assert "removed" in py_delta
        assert 42 in py_delta["added"]
        assert len(py_delta["removed"]) == 0
