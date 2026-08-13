"""A frame store implemented in Python (RFC 0016's seam, RFC 0019 step 6).

The native recorder writes through ``FrameStoreOps``. Binding a Python object
to that seam is what lets the data-frame adaptor's ``DataFrameStorage`` BACK
the native recorder instead of reimplementing one beside it - which is the
whole point of the migration: the adaptor keeps its storage surface and loses
only the part that was a second recorder.
"""

import hgraph as hg
import pytest
from hgraph import _hgraph


class RecordingStore:
    """The smallest thing satisfying the store contract."""

    def __init__(self):
        self.frames = {}
        self.writes = []

    def write(self, key, frame):
        self.writes.append(key)
        self.frames[key] = frame

    def read(self, key):
        return self.frames.get(key)

    def contains(self, key):
        return key in self.frames

    def clear(self):
        self.frames.clear()


@pytest.fixture
def store():
    s = RecordingStore()
    _hgraph._set_frame_store(s)
    try:
        yield s
    finally:
        # Back to the built-in in-memory store, or later tests inherit this one.
        _hgraph._clear_frame_store()


def _record(store, ticks, tp, key="out"):
    @hg.graph
    def g(ts: tp):
        hg.record(ts, key=key, recordable_id="py")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(g, ticks)


def test_the_native_recorder_writes_into_a_python_store(store):
    _record(store, [{"a": 1.0, "b": 2.0}, {"a": 3.0}], hg.TSD[str, hg.TS[float]])

    assert store.writes == ["py.out"]
    frame = store.frames["py.out"]
    assert list(frame.schema.names) == ["__date_time__", "__as_of__", "__key_1__", "value"]
    # One row per key that ticked - the native shape, not a per-tick table.
    assert frame.num_rows == 3


def test_the_run_writes_once_rather_than_per_tick(store):
    """The default accumulates and writes at stop. A write per tick is the
    O(n^2) behaviour the native recorder exists to avoid."""
    _record(store, [1, 2, 3, 4, 5], hg.TS[int])

    assert len(store.writes) == 1


def test_native_reads_resolve_through_the_python_store(store):
    _record(store, [1, 2, 3], hg.TS[int])

    assert hg.frame_store_contains("py.out")
    assert hg.frame_store_read("py.out").num_rows == 3


def test_a_missing_key_reads_as_absent_rather_than_raising(store):
    """``read`` promises its native callers an empty frame for a missing key."""
    assert not hg.frame_store_contains("py.nothing")


def test_replay_reads_back_what_the_store_holds(store):
    """The round trip crosses the seam twice - written through it, read back
    through it - so a store that only half-works fails here."""
    ticks = [{"a": 1.0, "b": 2.0}, {"a": 3.0}]

    @hg.graph
    def rec(ts: hg.TSD[str, hg.TS[float]]):
        hg.record(ts, key="rt", recordable_id="py")

    @hg.graph
    def rep() -> hg.TSD[str, hg.TS[float]]:
        return hg.replay("rt", hg.TSD[str, hg.TS[float]], recordable_id="py")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(rec, ticks)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert hg.eval_node(rep) == ticks
