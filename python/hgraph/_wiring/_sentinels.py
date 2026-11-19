"""Delta sentinels shared across the wiring layer.

``REMOVE``/``Removed``/``_SetDelta``/``Null`` are identity-critical: they are
handed to the C++ bridge at package import (``_hgraph._set_removed_sentinel``
and friends) so class identity — not equality — shapes TSS/TSD delta
application and the unset state of an ``Optional`` value.
Define them here once; every other module re-exports."""

from functools import lru_cache

import numpy as np

import _hgraph


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


class _Null:
    """hgraph's ``Null`` marker (RFC 0050): the unset state of the value of
    a ``TS[Optional[T]]``.

    A node that returns ``Null`` ticks a value that is unset: the output is
    valid and modified, and holds no payload. ``None`` is still "no tick".
    A reader sees ``None`` as the ``value`` of such a tick and ``Null`` as its
    ``delta_value``, where ``None`` means "not modified"."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "Null"

    def __bool__(self):
        return False

    def __reduce__(self):
        # One instance, compared by identity, in every process.
        return (_Null, ())


Null = _Null()


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


class TSLDelta(dict):
    """Positional list updates. A size-only delta has no positional entries."""

    def __init__(self, modified=None, *, removed=(), size_modified=None):
        super().__init__(modified or {})
        for index in removed:
            if index in self and self[index] is not REMOVE:
                raise ValueError(f"position {index} is both modified and removed")
            self[index] = REMOVE
        self.size_modified = size_modified

    @property
    def modified(self):
        return {k: v for k, v in self.items() if v is not REMOVE}

    @property
    def removed(self):
        return frozenset(k for k, v in self.items() if v is REMOVE)

    def __bool__(self):
        return bool(len(self)) or self.size_modified is not None

    def copy(self):
        return TSLDelta(self, size_modified=self.size_modified)

    def __eq__(self, other):
        if isinstance(other, TSLDelta) and self.size_modified != other.size_modified:
            return False
        return dict.__eq__(self, other)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return f"TSLDelta({self.modified!r}, removed={self.removed!r}, size_modified={self.size_modified!r})"


@lru_cache
def _bundle_delta_fields(ts_type):
    return dict(_hgraph.ts_field_types(ts_type))


def _simplify_delta(value, ts_type):
    """Present a native delta using the declared time-series type at each position."""
    if not isinstance(value, dict):
        return value
    if ts_type.kind == _hgraph.TS_KIND_TSW:
        # eval_node historically returns the arriving sample. Raw recordings and
        # window_delta retain the complete change, including departure-only ticks.
        replacement = value.get("replacement")
        if replacement is not None and len(replacement):
            return replacement[-1]["value"]
        if not len(value["added"]):
            return None
        arrival = value["added"][0]
        return arrival.item() if isinstance(arrival, np.generic) else arrival
    if ts_type.is_tsl:
        child = _hgraph.tsl_element_ts(ts_type)
        modified = {i: _simplify_delta(v, child) for i, v in value["modified"].items()}
        if ts_type.is_fixed_tsl:
            modified.update(dict.fromkeys(value["removed"], REMOVE))
            return modified
        return TSLDelta(modified, removed=value["removed"], size_modified=value["size_modified"])
    if ts_type.is_tsd:
        child = _hgraph.tsd_element_ts(ts_type)
        modified = {k: _simplify_delta(v, child) for k, v in value["modified"].items()}
        modified.update(dict.fromkeys(value["removed"], REMOVE))
        return modified
    if ts_type.is_tsb:
        fields = _bundle_delta_fields(ts_type)
        return {k: _simplify_delta(v, fields[k]) for k, v in value.items()}
    if ts_type.is_tss:
        return _SetDelta(added=value["added"], removed=value["removed"])
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
