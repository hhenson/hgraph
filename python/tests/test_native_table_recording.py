"""Native recording of partitioned time-series (RFC 0019).

Recording a ``TSD`` used to fail at ``start``::

    table codec: unsupported value kind for 'Map[str,float]'
      (atomics and depth-1 bundles in v1)

because the recorder was built from a value schema, which cannot describe a
``TSD``'s key columns or removed flags. It is built from the table layout now,
which already models them.
"""

import hgraph as hg


def _record(ticks, tp):
    @hg.graph
    def g(ts: tp):
        hg.record(ts, key="out", recordable_id="native")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(g, ticks)
        return hg.frame_store_read("native.out")


def _columns(frame):
    return list(frame.schema.names() if callable(frame.schema.names) else frame.schema.names)


def _rows(frame):
    return frame.num_rows if hasattr(frame, "num_rows") else frame.height


def test_a_tsd_records_natively():
    """The shape that had no native path at all."""
    frame = _record(
        [{"a": 1.0, "b": 2.0}, {"a": 3.0}], hg.TSD[str, hg.TS[float]]
    )

    assert _columns(frame) == ["__date_time__", "__as_of__", "__key_1__", "value"]
    # One row per key that ticked, not one row per tick.
    assert _rows(frame) == 3


def test_the_key_column_carries_the_partition_key():
    frame = _record([{"a": 1.0, "b": 2.0}], hg.TSD[str, hg.TS[float]])

    keys = frame["__key_1__"]
    assert sorted(keys.to_pylist() if hasattr(keys, "to_pylist") else keys.to_list()) == ["a", "b"]


def test_removals_are_omitted_by_default():
    """``removes`` defaults to Omit, matching the adaptor's ``_OverrideState``.

    A removal means nothing further is recorded for that key: no removed-flag
    column, and no row - not a row of nulls.
    """
    frame = _record(
        [{"a": 1.0}, {"a": hg.REMOVE_IF_EXISTS}], hg.TSD[str, hg.TS[float]]
    )

    assert "__key_1_removed__" not in _columns(frame)
    assert _rows(frame) == 1


def test_a_flat_series_still_records():
    """The no-levels case goes through the same recorder."""
    frame = _record([1, 2, 3], hg.TS[int])

    assert _columns(frame) == ["__date_time__", "__as_of__", "value"]
    assert _rows(frame) == 3


def _round_trip(ticks, tp):
    @hg.graph
    def rec(ts: tp):
        hg.record(ts, key="out", recordable_id="rt")

    @hg.graph
    def rep() -> tp:
        return hg.replay("out", tp, recordable_id="rt")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(rec, ticks)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            return hg.eval_node(rep)


def test_a_tsd_round_trips_through_record_and_replay():
    """Replay reconstructs the partition, not just the value.

    A recorded row is a key and a value rather than a whole value, so it cannot
    be read straight back through a converter - replay descends the levels the
    row names and applies at that key.
    """
    ticks = [{"a": 1.0, "b": 2.0}, {"a": 3.0}]

    assert _round_trip(ticks, hg.TSD[str, hg.TS[float]]) == ticks


def test_a_flat_series_still_round_trips():
    assert _round_trip([1, 2, 3], hg.TS[int]) == [1, 2, 3]
