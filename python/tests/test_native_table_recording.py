"""Native recording of partitioned time-series (RFC 0019).

Recording a ``TSD`` used to fail at ``start``::

    table codec: unsupported value kind for 'Map[str,float]'
      (atomics and depth-1 bundles in v1)

because the recorder was built from a value schema, which cannot describe a
``TSD``'s key columns or removed flags. It is built from the table layout now,
which already models them.
"""

import hgraph as hg


def _record(ticks, tp, **config):
    @hg.graph
    def g(ts: tp):
        hg.record(ts, key="out", recordable_id="native", **config)

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


# --------------------------------------------------------------------------
# Local configuration (RFC 0019, "Configuration is local, with a global
# default"). The options were reachable only as C++ defaults constructed
# inside ``start``; these pin them as arguments at the call site.
# --------------------------------------------------------------------------


def test_removals_are_recorded_when_tracked():
    """The counterpart to ``test_removals_are_omitted_by_default``."""
    frame = _record(
        [{"a": 1.0}, {"a": hg.REMOVE_IF_EXISTS}],
        hg.TSD[str, hg.TS[float]],
        removes=hg.RecordRemoves.TRACK,
    )

    assert "__key_1_removed__" in _columns(frame)
    # The removal is now its own row, flagged - rather than nothing at all.
    assert _rows(frame) == 2
    removed = frame["__key_1_removed__"]
    assert (removed.to_pylist() if hasattr(removed, "to_pylist") else removed.to_list()) == [False, True]


def test_the_as_of_column_can_be_omitted():
    frame = _record([1, 2], hg.TS[int], as_of=hg.RecordAsOf.OMIT)

    assert _columns(frame) == ["__date_time__", "value"]
    # Dropping a column must not shift the ones after it.
    values = frame["value"]
    assert (values.to_pylist() if hasattr(values, "to_pylist") else values.to_list()) == [1, 2]


def test_inherit_records_exactly_as_an_unconfigured_call():
    """The default must stay a no-op, or every existing recording changes."""
    plain = _record([1, 2], hg.TS[int])
    inherited = _record(
        [1, 2], hg.TS[int], as_of=hg.RecordAsOf.INHERIT, removes=hg.RecordRemoves.INHERIT
    )

    assert _columns(plain) == _columns(inherited)
    assert _rows(plain) == _rows(inherited)


def test_two_recordings_in_one_graph_differ_by_configuration():
    """The point of local config: they differ by being CALLED differently,
    not by a registry keyed on their name."""

    @hg.graph
    def g(ts: hg.TSD[str, hg.TS[float]]):
        hg.record(ts, key="tracked", recordable_id="native",
                  removes=hg.RecordRemoves.TRACK)
        hg.record(ts, key="plain", recordable_id="native")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(g, [{"a": 1.0}, {"a": hg.REMOVE_IF_EXISTS}])
        tracked = hg.frame_store_read("native.tracked")
        plain = hg.frame_store_read("native.plain")

    assert "__key_1_removed__" in _columns(tracked)
    assert "__key_1_removed__" not in _columns(plain)
    assert _rows(tracked) == 2
    assert _rows(plain) == 1
