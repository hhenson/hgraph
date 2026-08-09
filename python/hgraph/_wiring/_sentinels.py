"""Delta sentinels shared across the wiring layer.

``REMOVE``/``Removed``/``_SetDelta`` are identity-critical: they are handed
to the C++ bridge at package import (``_hgraph._set_removed_sentinel`` and
friends) so class identity — not equality — shapes TSS/TSD delta application.
Define them here once; every other module re-exports."""

# ``reduce(func, ts)`` has no zero, whereas ``reduce(func, ts, None)`` supplies
# a typed, never-valid zero. Keep omission distinct without exposing another
# public marker.
_REDUCE_ZERO = object()


class _Removed:
    """hgraph's REMOVE marker: a TSD key removal (input) / removed key
    (test read-back)."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "REMOVE"


REMOVE = _Removed()


class _RemovedIfExists:
    """hgraph's REMOVE_IF_EXISTS marker: a lenient TSD key removal — unlike
    ``REMOVE``, an absent key is silently ignored at delta application."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "REMOVE_IF_EXISTS"


REMOVE_IF_EXISTS = _RemovedIfExists()


class Removed:
    """hgraph's TSS removal wrapper: Removed(item) marks a removed set
    element in a delta; hashes/compares as the item (hgraph parity)."""

    __slots__ = ("item",)

    def __init__(self, item):
        self.item = item

    def __hash__(self):
        return hash(self.item)

    def __eq__(self, other):
        return self.item == other.item if type(other) is Removed else self.item == other

    def __repr__(self):
        return f"Removed({self.item!r})"


def _simplify_delta(value):
    """Map canonical delta bundles back to hgraph's friendly test shapes:
    TSD {removed, modified} -> {key: value, removed_key: REMOVE};
    TSS {added, removed} -> SetDelta."""
    if isinstance(value, dict):
        if set(value.keys()) in ({"removed", "modified"}, {"removed", "modified", "removed_strict"}):
            out = {k: _simplify_delta(v) for k, v in value["modified"].items()}
            out.update({k: REMOVE for k in value["removed"]})
            out.update({k: REMOVE for k in value.get("removed_strict", ())})
            return out
        if set(value.keys()) == {"added", "removed"}:
            # hgraph's TSS delta shape: one set - added items plain,
            # removed items wrapped in Removed(...).
            return _SetDelta(added=value["added"], removed=value["removed"])
        return {k: _simplify_delta(v) for k, v in value.items()}
    return value

class _SetDelta(frozenset):
    """hgraph's SetDelta: a frozenset of added items + Removed(...) markers.
    A frozenset SUBCLASS so equality/iteration/conversion behave like the
    friendly shape, while staying distinguishable from a plain frozenset
    (which a TSS node return applies as the FULL VALUE, upstream parity).

    The added/removed fields are stored EXPLICITLY (upstream keeps them as
    separate frozensets) and MUST be disjoint (ruling 2026-07-28): an
    element listed in both is incorrect data, rejected here at
    construction. Enforcement lives at this boundary because the frozenset
    content could not even represent the bad state faithfully — Removed(x)
    hashes as x, so the union silently collapses the pair (issues
    #148/#161/#162)."""

    __slots__ = ("_added", "_removed")

    def __new__(cls, iterable=(), *, added=None, removed=None):
        if added is None and removed is None:
            added = frozenset(e for e in iterable if type(e) is not Removed)
            removed = frozenset(e.item for e in iterable if type(e) is Removed)
        else:
            added = frozenset(added) if added else frozenset()
            removed = frozenset(removed) if removed else frozenset()
        overlap = added & removed
        if overlap:
            raise ValueError(
                f"a set delta cannot both add and remove the same element(s): {sorted(overlap, key=repr)!r}")
        self = super().__new__(cls, added | {Removed(r) for r in removed})
        self._added = added
        self._removed = removed
        return self

    @property
    def added(self):
        return self._added

    @property
    def removed(self):
        return self._removed

    def __add__(self, other):
        if isinstance(other, _SetDelta):
            other_added, other_removed = other.added, other.removed
        else:
            other_added = frozenset(e for e in other if type(e) is not Removed)
            other_removed = frozenset(e.item for e in other if type(e) is Removed)
        # upstream PythonSetDelta.__add__ composition rules
        added = (self.added - other_removed) | other_added
        removed = (other_removed - self.added) | (self.removed - other_added)
        return _SetDelta(added=added, removed=removed)


def set_delta(added=None, removed=None, tp=None):
    """hgraph's set-delta literal: the friendly TSS delta shape - added
    items plain, removals wrapped in Removed."""
    return _SetDelta(added=added, removed=removed)


def compute_set_delta(value, out):
    """Delta between the node's CURRENT output set and the new target set
    (hgraph parity: use with the _output injection)."""
    target = frozenset(value.value if hasattr(value, "value") else value)
    if out is not None and out.valid:
        current = frozenset(out.value)
        return set_delta(added=target - current, removed=current - target)
    return set_delta(added=target)
