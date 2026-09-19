"""The graphs a recovery scenario is assembled from.

Everything a worker process has to import lives here under a stable, module-level name,
because a ``dmap_`` child and a ``spawn_`` stage are found again in the worker by module
and qualified name. Composite graphs are named by their structure --
``dmapp__map__total`` is ``dmap_`` over processes, of ``map_``, of ``total`` -- and are
built on first use by the module's ``__getattr__``, identically on both sides.
"""

from __future__ import annotations

import os
import pickle
from dataclasses import dataclass

import hgraph as hg

SEPARATOR = "__"
MAX_DEPTH = 3


class _Total(hg.TimeSeriesSchema):
    value: hg.TS[int]


class _Calls(hg.TimeSeriesSchema):
    calls: hg.TS[int]


@hg.compute_node
def total(ts: hg.TS[int], state: hg.RECORDABLE_STATE[_Total] = None) -> hg.TS[int]:
    """State that a lost, repeated or re-ticked cycle changes."""
    state.value.value = (state.value.value if state.value.valid else 0) + ts.value
    return state.value.value


@hg.compute_node
def doubled(ts: hg.TS[int]) -> hg.TS[int]:
    return ts.value * 2


@hg.sink_node
def note(ts: hg.TS[int], _state: hg.STATE = None):
    """A transient sink: ordinary state, no recordable state. Recovery leaves it alone."""
    _state.seen = getattr(_state, "seen", 0) + 1


@hg.graph
def audited(ts: hg.TS[int]) -> hg.TS[int]:
    """The recoverable total with a transient sink beside it."""
    out = total(ts)
    note(out)
    return out


@hg.component
def hosted(ts: hg.TS[int]) -> hg.TS[int]:
    """The recoverable unit when the component is INSIDE the worker."""
    return total(ts)


@hg.compute_node
def _combine(lhs: hg.TS[int], rhs: hg.TS[int], state: hg.RECORDABLE_STATE[_Calls] = None) -> hg.TS[int]:
    # Operand order and the number of real evaluations both show in the output.
    state.calls.value = (state.calls.value if state.calls.valid else 0) + 1
    return lhs.value * 10 + rhs.value + state.calls.value


@hg.graph
def combine(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return _combine(lhs, rhs)


@hg.compute_node
def _settle(ts: hg.TS[int]) -> hg.TS[int]:
    return ts.value


@hg.graph
def folded(ts: hg.TSD[int, hg.TS[int]]) -> hg.TS[int]:
    """A stateful reduction. It ends in a node that writes its own output, which is the
    ``map_`` child form that can be checkpointed today."""
    return _settle(hg.reduce(combine, ts, 0))


@hg.graph
def summed(ts: hg.TSD[int, hg.TS[int]]) -> hg.TS[int]:
    """A STATELESS reduction: its output is a function of the current input alone, so
    re-seeding the inputs (RECOVER mode) is enough to bring it back."""
    return _settle(hg.reduce(lambda lhs, rhs: lhs + rhs, ts, 0))


def canonical(value):
    """A delta as plain, ordered, comparable data."""
    if value is hg.REMOVE or value is getattr(hg, "REMOVE_IF_EXISTS", None):
        return "REMOVE"
    if hasattr(value, "items"):
        return tuple(sorted(((key, canonical(item)) for key, item in value.items()), key=repr))
    if isinstance(value, (set, frozenset)):
        return tuple(sorted((canonical(item) for item in value), key=repr))
    return value


@hg.sink_node
def record(value: hg.TIME_SERIES_TYPE, path: str, clock: hg.CLOCK = None):
    """The sink a pipeline ends in. It acts in a worker process, asks for the clock and
    declares nothing: one file per process, read back in time order."""
    with open(f"{path}.{os.getpid()}", "ab") as stream:
        pickle.dump((clock.evaluation_time, canonical(value.delta_value)), stream)


@dataclass(frozen=True)
class Entry:
    name: str
    fn: object
    input: object
    output: object
    depth: int          # how many keyed levels the INPUT has
    stateful: bool      # does a lost restart show in the output?


LEAVES = {
    "total": Entry("total", total, hg.TS[int], hg.TS[int], 0, True),
    "doubled": Entry("doubled", doubled, hg.TS[int], hg.TS[int], 0, False),
    "audited": Entry("audited", audited, hg.TS[int], hg.TS[int], 0, True),
    "hosted": Entry("hosted", hosted, hg.TS[int], hg.TS[int], 0, True),
    "folded": Entry("folded", folded, hg.TSD[int, hg.TS[int]], hg.TS[int], 1, True),
    "summed": Entry("summed", summed, hg.TSD[int, hg.TS[int]], hg.TS[int], 1, False),
}

LAYERS = {
    "map": lambda child, ts: hg.map_(child, ts),
    "mesh": lambda child, ts: hg.mesh_(child, ts),
    "dmapi": lambda child, ts: hg.dmap_(child, ts, __workers__=2, in_process=True),
    "dmapp": lambda child, ts: hg.dmap_(child, ts, __workers__=2),
}

_CATALOGUE: dict[str, Entry] = dict(LEAVES)


def resolve(name: str) -> Entry:
    """The graph ``name`` describes, built on first use."""
    entry = _CATALOGUE.get(name)
    if entry is not None:
        return entry
    layer, separator, rest = name.partition(SEPARATOR)
    if not separator or layer not in LAYERS:
        raise AttributeError(name)
    child = resolve(rest)
    if child.depth + 1 > MAX_DEPTH:
        raise AttributeError(f"{name}: deeper than {MAX_DEPTH}")

    def lifted(ts):
        return LAYERS[layer](child.fn, ts)

    lifted.__name__ = lifted.__qualname__ = name
    lifted.__module__ = __name__
    lifted.__annotations__ = {"ts": hg.TSD[int, child.input], "return": hg.TSD[int, child.output]}
    entry = Entry(name, hg.graph(lifted), hg.TSD[int, child.input], hg.TSD[int, child.output],
                  child.depth + 1, child.stateful)
    _CATALOGUE[name] = entry
    return entry


def __getattr__(name: str):
    # A worker process finds a composite by ``getattr(module, name)``.
    return resolve(name).fn
