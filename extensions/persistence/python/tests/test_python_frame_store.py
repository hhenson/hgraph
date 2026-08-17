"""A frame store implemented in Python (RFC 0016's seam, RFC 0019 step 6).

The native recorder writes through the graph-scoped ``FrameStore``. Binding a
Python object to that seam is what lets the data-frame adaptor's
``DataFrameStorage`` back
the native recorder instead of reimplementing one beside it - which is the
whole point of the migration: the adaptor keeps its storage surface and loses
only the part that was a second recorder.
"""

import hgraph as hg
import pyarrow as pa
import pytest
from contextlib import contextmanager
from hgraph import _hgraph
import hgraph_persistence as _hgraph_persistence


class RecordingStore:
    """The smallest thing satisfying the store contract."""

    def __init__(self):
        self.frames = {}
        self.writes = []

    def store(self, key, frame):
        self.writes.append(key)
        self.frames[key] = frame

    def load(self, key):
        return self.frames.get(key)

    def has(self, key):
        return key in self.frames


@pytest.fixture
def store():
    return RecordingStore()


@contextmanager
def _using(store):
    with hg.GlobalState() as state:
        hg.set_record_replay_model("DataFrame")
        _hgraph_persistence._hgraph_persistence._set_python_frame_store(state._impl, store)
        try:
            yield
        finally:
            _hgraph_persistence._hgraph_persistence._restore_python_frame_store(state._impl)


def _record(store, ticks, tp, key="out", **options):
    @hg.graph
    def g(ts: tp):
        hg.record(ts, key=key, recordable_id="py", **options)

    with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
        hg.eval_node(g, ticks)


def test_the_native_recorder_writes_into_a_python_store(store):
    with _using(store):
        _record(store, [{"a": 1.0, "b": 2.0}, {"a": 3.0}], hg.TSD[str, hg.TS[float]])

    assert store.writes == ["py.out"]
    frame = store.frames["py.out"]
    assert list(frame.schema.names) == ["__date_time__", "__as_of__", "__key_1__", "value"]
    # One row per key that ticked - the native shape, not a per-tick table.
    assert frame.num_rows == 3


def test_python_store_preserves_projection_metadata_with_polars_presentation(store):
    """The store protocol is internal and always receives Arrow.

    User-facing Polars presentation discards Arrow schema metadata. Passing a
    recording through that presentation would therefore erase the only
    authoritative distinction between an omitted optional column and one
    recorded under a non-default name. Store timestamps retain the established
    naive-UTC Python compatibility form.
    """

    @hg.graph
    def rep() -> hg.TS[int]:
        return hg.replay("out", hg.TS[int], recordable_id="py")

    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(True)
    try:
        with _using(store):
            _record(store, [1, 2], hg.TS[int], as_of_key="revision")
            frame = store.frames["py.out"]
            assert isinstance(frame, pa.Table)
            assert frame.schema.metadata[
                b"hgraph.recording.projection.column.1"
            ] == b"revision"
            assert all(
                field.type.tz is None
                for field in frame.schema
                if pa.types.is_timestamp(field.type)
            )

            with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
                with pytest.raises(
                    Exception, match="stored projection names that column 'revision'"
                ):
                    hg.eval_node(rep)
    finally:
        _hgraph.set_polars_frames(previous)


def test_the_run_writes_once_rather_than_per_tick(store):
    """The default accumulates and writes at stop. A write per tick is the
    O(n^2) behaviour the native recorder exists to avoid."""
    with _using(store):
        _record(store, [1, 2, 3, 4, 5], hg.TS[int])

    assert len(store.writes) == 1


def test_python_compatibility_stores_remain_whole_frame_when_flushing_is_requested(store):
    """Segmentation is a native-store protocol, not an expansion of the
    Python ``store/load/has`` compatibility seam."""
    with _using(store):
        _record(store, [1, 2, 3], hg.TS[int], flush_rows=1)

    assert store.writes == ["py.out"]
    assert store.frames["py.out"]["value"].to_pylist() == [1, 2, 3]


def test_native_reads_resolve_through_the_python_store(store):
    with _using(store):
        _record(store, [1, 2, 3], hg.TS[int])
        assert hg.frame_store_contains("py.out")
        assert hg.frame_store_read("py.out").num_rows == 3


def test_reselecting_the_native_model_starts_with_a_fresh_store():
    """The implicit Python GlobalState outlives one eval_node call. Explicit
    model selection denotes a new native recording session, so immutable keys
    from an earlier session must not leak into it."""
    with hg.GlobalState():
        hg.set_record_replay_model(hg.DATA_FRAME)
        _record(None, [1], hg.TS[int])
        assert hg.frame_store_contains("py.out")

        hg.set_record_replay_model(hg.IN_MEMORY)
        hg.set_record_replay_model(hg.DATA_FRAME)
        assert not hg.frame_store_contains("py.out")
        _record(None, [2], hg.TS[int])


def test_reselecting_the_model_keeps_a_python_compatibility_store(store):
    with hg.GlobalState() as state:
        hg.set_record_replay_model(hg.DATA_FRAME)
        _hgraph_persistence._hgraph_persistence._set_python_frame_store(state._impl, store)
        try:
            hg.set_record_replay_model(hg.DATA_FRAME)
            _record(store, [1], hg.TS[int])
        finally:
            _hgraph_persistence._hgraph_persistence._restore_python_frame_store(state._impl)

    assert store.writes == ["py.out"]


def test_a_missing_key_reads_as_absent_rather_than_raising(store):
    """``read`` promises its native callers an empty frame for a missing key."""
    with _using(store):
        assert not hg.frame_store_contains("py.nothing")


def test_python_delegation_leaves_overwrite_policy_to_python(store):
    """Unlike native stores, the compatibility seam imposes no immutability."""
    with _using(store):
        _record(store, [1], hg.TS[int])
        _record(store, [2], hg.TS[int])
    assert store.writes == ["py.out", "py.out"]
    assert store.frames["py.out"]["value"].to_pylist() == [2]


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

    with _using(store):
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(rec, ticks)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert hg.eval_node(rep) == ticks


def test_nested_python_stores_restore_the_previous_graph_store():
    first = RecordingStore()
    second = RecordingStore()

    with hg.GlobalState() as state:
        hg.set_record_replay_model("DataFrame")
        _hgraph_persistence._hgraph_persistence._set_python_frame_store(state._impl, first)
        _hgraph_persistence._hgraph_persistence._set_python_frame_store(state._impl, second)
        _hgraph_persistence._hgraph_persistence._restore_python_frame_store(state._impl)

        _record(first, [1], hg.TS[int], key="first")
        assert first.writes == ["py.first"]
        assert second.writes == []

        _hgraph_persistence._hgraph_persistence._restore_python_frame_store(state._impl)
        _record(first, [2], hg.TS[int], key="native")
        assert first.writes == ["py.first"]
        assert hg.frame_store_contains("py.native")


def test_stored_replay_filters_start_time_and_selects_the_visible_revision(store):
    @hg.graph
    def rep() -> hg.TS[int]:
        return hg.replay("values", hg.TS[int], recordable_id="py")

    store.frames["py.values"] = pa.table(
        {
            "__date_time__": [
                hg.MIN_ST,
                hg.MIN_ST,
                hg.MIN_ST + hg.MIN_TD,
                hg.MIN_ST + hg.MIN_TD,
            ],
            "__as_of__": [
                hg.MIN_ST + 20 * hg.MIN_TD,
                hg.MIN_ST + 10 * hg.MIN_TD,
                hg.MIN_ST + 20 * hg.MIN_TD,
                hg.MIN_ST + 10 * hg.MIN_TD,
            ],
            "value": [20, 10, 40, 30],
        }
    )

    with _using(store):
        hg.set_as_of(hg.MIN_ST + 15 * hg.MIN_TD)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert hg.eval_node(rep) == [10, 30]

    with _using(store):
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert hg.eval_node(
                rep, __start_time__=hg.MIN_ST + hg.MIN_TD
            ) == [40]
