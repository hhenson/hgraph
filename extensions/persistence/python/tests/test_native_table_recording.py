"""Native recording of partitioned time-series (RFC 0019).

Recording a ``TSD`` used to fail at ``start``::

    table codec: unsupported value kind for 'Map[str,float]'
      (atomics and depth-1 bundles in v1)

because the recorder was built from a value schema, which cannot describe a
``TSD``'s key columns or removed flags. It is built from the table layout now,
which already models them.
"""

import hgraph as hg
import pytest

# The durable recording vocabularies are extension-owned (RFC 0025
# checkpoint 5); hgraph.RecordAsOf/RecordRemoves are deprecated aliases.
from hgraph_persistence import RecordAsOf, RecordRemoves


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


def test_an_omitted_removal_does_not_remove_the_replayed_key():
    ticks = [{"a": 1.0}, {"a": hg.REMOVE_IF_EXISTS}]

    # There is deliberately no second replay tick: omission records neither a
    # removal flag nor a row, so replay retains the last value for the key.
    assert _round_trip(ticks, hg.TSD[str, hg.TS[float]]) == [{"a": 1.0}]


def test_a_flat_series_still_records():
    """The no-levels case goes through the same recorder."""
    frame = _record([1, 2, 3], hg.TS[int])

    assert _columns(frame) == ["__date_time__", "__as_of__", "value"]
    assert _rows(frame) == 3


def test_record_accepts_tick_sample_and_snap_table_modes():
    ticks = [{"a": 1, "b": 10}, {"a": 2}]
    tp = hg.TSD[str, hg.TS[int]]

    tick = _record(ticks, tp, mode=hg.ToTableMode.Tick)
    sample = _record(ticks, tp, mode=hg.ToTableMode.Sample)
    snap = _record(ticks, tp, mode=hg.ToTableMode.Snap)

    assert _rows(tick) == 3
    assert _rows(sample) == 3
    # Snap walks the complete valid dictionary on the second tick, including
    # key b, while Tick and Sample walk only modified keys.
    assert _rows(snap) == 4


def _round_trip(ticks, tp, replay_config=None, **config):
    @hg.graph
    def rec(ts: tp):
        hg.record(ts, key="out", recordable_id="rt", **config)

    @hg.graph
    def rep() -> tp:
        effective_replay_config = replay_config
        if effective_replay_config is None:
            effective_replay_config = {
                name: config[name]
                for name in (
                    "partition_names",
                    "removed_names",
                    "date_key",
                    "as_of_key",
                    "frame_prefix",
                )
                if name in config
            }
        return hg.replay("out", tp, recordable_id="rt", **effective_replay_config)

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


def test_an_outer_nested_tsd_removal_round_trips_by_its_key_prefix():
    """An outer removal has no descendant key, by construction."""
    ticks = [{"desk": {"a": 1.0, "b": 2.0}}, {"desk": hg.REMOVE}]

    assert _round_trip(
        ticks,
        hg.TSD[str, hg.TSD[str, hg.TS[float]]],
        removes=RecordRemoves.TRACK,
    ) == ticks


def test_a_flat_series_still_round_trips():
    assert _round_trip([1, 2, 3], hg.TS[int]) == [1, 2, 3]


def test_per_recording_time_column_names_round_trip():
    frame = _record(
        [1, 2], hg.TS[int], date_key="event_time", as_of_key="revision"
    )
    assert _columns(frame) == ["event_time", "revision", "value"]
    assert _round_trip(
        [1, 2], hg.TS[int], date_key="event_time", as_of_key="revision"
    ) == [1, 2]


def test_renamed_partition_and_removal_columns_round_trip():
    ticks = [{"a": 1.0}, {"a": hg.REMOVE}]
    assert _round_trip(
        ticks,
        hg.TSD[str, hg.TS[float]],
        removes=RecordRemoves.TRACK,
        partition_names=("symbol",),
        removed_names=("symbol_gone",),
    ) == ticks


def test_replay_refuses_to_omit_a_renamed_as_of_projection():
    with pytest.raises(Exception, match="stored projection names that column 'revision'"):
        _round_trip(
            [1, 2],
            hg.TS[int],
            replay_config={},
            as_of_key="revision",
        )


def test_replay_refuses_to_omit_a_renamed_removal_projection():
    ticks = [{"a": 1.0}, {"a": hg.REMOVE}]
    with pytest.raises(Exception, match="stored projection names that column 'gone'"):
        _round_trip(
            ticks,
            hg.TSD[str, hg.TS[float]],
            replay_config={},
            removes=RecordRemoves.TRACK,
            removed_names=("gone",),
        )


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
        removes=RecordRemoves.TRACK,
    )

    assert "__key_1_removed__" in _columns(frame)
    # The removal is now its own row, flagged - rather than nothing at all.
    assert _rows(frame) == 2
    removed = frame["__key_1_removed__"]
    assert (removed.to_pylist() if hasattr(removed, "to_pylist") else removed.to_list()) == [False, True]


def test_the_as_of_column_can_be_omitted():
    frame = _record([1, 2], hg.TS[int], as_of=RecordAsOf.OMIT)

    assert _columns(frame) == ["__date_time__", "value"]
    # Dropping a column must not shift the ones after it.
    values = frame["value"]
    assert (values.to_pylist() if hasattr(values, "to_pylist") else values.to_list()) == [1, 2]


def test_an_omitted_as_of_column_still_round_trips():
    assert _round_trip(
        [1, 2], hg.TS[int], as_of=RecordAsOf.OMIT
    ) == [1, 2]


def test_explicit_track_overrides_a_fixed_graph_default():
    @hg.graph
    def g(ts: hg.TS[int]):
        hg.record(ts, key="out", recordable_id="native", as_of=RecordAsOf.TRACK)

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        hg.set_as_of(hg.MIN_ST + 10 * hg.MIN_TD)
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(g, [1, 2])
        frame = hg.frame_store_read("native.out")

    values = frame["__as_of__"]
    values = values.to_pylist() if hasattr(values, "to_pylist") else values.to_list()
    assert values == [hg.MIN_ST, hg.MIN_ST + hg.MIN_TD]


def test_inherit_records_exactly_as_an_unconfigured_call():
    """The default must stay a no-op, or every existing recording changes."""
    plain = _record([1, 2], hg.TS[int])
    inherited = _record(
        [1, 2], hg.TS[int], as_of=RecordAsOf.INHERIT, removes=RecordRemoves.INHERIT
    )

    assert _columns(plain) == _columns(inherited)
    assert _rows(plain) == _rows(inherited)


def test_two_recordings_in_one_graph_differ_by_configuration():
    """The point of local config: they differ by being CALLED differently,
    not by a registry keyed on their name."""

    @hg.graph
    def g(ts: hg.TSD[str, hg.TS[float]]):
        hg.record(ts, key="tracked", recordable_id="native",
                  removes=RecordRemoves.TRACK)
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


def test_partition_columns_can_be_renamed():
    """The native equivalent of the adaptor's ``partition_keys`` override."""
    frame = _record(
        [{"a": 1.0}], hg.TSD[str, hg.TS[float]], partition_names=("symbol",)
    )

    assert _columns(frame) == ["__date_time__", "__as_of__", "symbol", "value"]


def test_removed_columns_can_be_renamed():
    frame = _record(
        [{"a": 1.0}, {"a": hg.REMOVE_IF_EXISTS}],
        hg.TSD[str, hg.TS[float]],
        removes=RecordRemoves.TRACK,
        partition_names=("symbol",),
        removed_names=("symbol_gone",),
    )

    assert _columns(frame) == ["__date_time__", "__as_of__", "symbol_gone", "symbol", "value"]


def test_a_rename_list_of_the_wrong_length_is_refused():
    """One name per FLATTENED key column - a miscount would silently misname
    columns, so it raises rather than applying to the first N."""
    import pytest

    with pytest.raises(Exception, match="2 partition names for 1 flattened key column"):
        _record(
            [{"a": 1.0}],
            hg.TSD[str, hg.TS[float]],
            partition_names=("one", "two"),
        )


def test_a_rename_list_of_the_wrong_element_type_is_refused():
    """The argument is constrained to tuple[str, ...], not merely erased -
    without the constraint any scalar would bind and fail much later."""
    import pytest

    with pytest.raises(Exception, match="no matching overload"):
        _record([{"a": 1.0}], hg.TSD[str, hg.TS[float]], partition_names=(1, 2))


def test_the_frame_columns_of_a_frame_valued_leaf_can_be_prefixed():
    """A frame-valued tick expands to one row per FRAME row, so the frame's
    own columns sit alongside the bitemporal ones. ``frame_prefix`` is what
    keeps them clear of each other, and of a second frame recorded beside
    them."""
    import pyarrow as pa
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        instrument: str
        value: float

    tick = pa.table({"instrument": ["a", "b"], "value": [1.0, 2.0]})
    frame = _record([tick], hg.TS[hg.Frame[PriceRow]], frame_prefix="px_")

    assert _columns(frame) == ["__date_time__", "__as_of__", "px_instrument", "px_value"]
    # One row per frame row, not one per tick.
    assert _rows(frame) == 2


def test_a_zero_row_frame_tick_is_rejected_instead_of_disappearing():
    import pyarrow as pa
    import pytest
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        instrument: str
        value: float

    tick = pa.table(
        {
            "instrument": pa.array([], type=pa.string()),
            "value": pa.array([], type=pa.float64()),
        }
    )
    with pytest.raises(Exception, match="zero-row Frame ticks cannot be recorded"):
        _record([tick], hg.TS[hg.Frame[PriceRow]])


def test_a_prefixed_frame_valued_leaf_round_trips_all_rows():
    import pyarrow as pa
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        instrument: str
        value: float

    tick = pa.table({"instrument": ["a", "b"], "value": [1.0, 2.0]})
    result = _round_trip(
        [tick], hg.TS[hg.Frame[PriceRow]], frame_prefix="px_"
    )

    assert len(result) == 1
    replayed = result[0]
    assert replayed["instrument"].to_pylist() == ["a", "b"]
    assert replayed["value"].to_pylist() == [1.0, 2.0]


def test_frame_valued_leaves_beneath_a_tsd_round_trip_by_key():
    import pyarrow as pa
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        instrument: str
        value: float

    first = pa.table({"instrument": ["a", "b"], "value": [1.0, 2.0]})
    second = pa.table({"instrument": ["c"], "value": [3.0]})
    replacement = pa.table({"instrument": ["d", "e"], "value": [4.0, 5.0]})
    ticks = [{"one": first, "two": second}, {"one": replacement}]

    result = _round_trip(ticks, hg.TSD[str, hg.TS[hg.Frame[PriceRow]]])

    assert len(result) == 2
    assert result[0]["one"].equals(first)
    assert result[0]["two"].equals(second)
    assert result[1]["one"].equals(replacement)


def test_to_table_from_table_round_trips_frame_valued_tsd_leaves():
    import pyarrow as pa
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        instrument: str
        value: float

    tp = hg.TSD[str, hg.TS[hg.Frame[PriceRow]]]

    @hg.graph
    def g(ts: tp) -> tp:
        return hg.from_table[tp](hg.to_table(ts))

    first = pa.table({"instrument": ["a", "b"], "value": [1.0, 2.0]})
    second = pa.table({"instrument": ["c"], "value": [3.0]})
    result = hg.eval_node(g, [{"one": first, "two": second}])

    assert result[0]["one"].equals(first)
    assert result[0]["two"].equals(second)


def test_a_keyed_frame_removal_round_trips():
    import pyarrow as pa
    from dataclasses import dataclass

    @dataclass(frozen=True)
    class PriceRow(hg.CompoundScalar):
        value: float

    frame = pa.table({"value": [1.0, 2.0]})
    ticks = [{"one": frame}, {"one": hg.REMOVE}]

    result = _round_trip(
        ticks,
        hg.TSD[str, hg.TS[hg.Frame[PriceRow]]],
        removes=RecordRemoves.TRACK,
    )

    assert result[0]["one"].equals(frame)
    assert result[1] == {"one": hg.REMOVE}


def test_native_segmented_recording_replays_across_all_committed_frames():
    @hg.graph
    def rec(ts: hg.TS[int]):
        hg.record(
            ts,
            key="out",
            recordable_id="segments",
            flush_rows=2,
        )

    @hg.graph
    def rep() -> hg.TS[int]:
        return hg.replay("out", hg.TS[int], recordable_id="segments")

    with hg.GlobalState():
        hg.set_record_replay_model("DataFrame")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(rec, [1, 2, 3, 4, 5])

        assert hg.frame_store_read("segments.out").num_rows == 0
        assert hg.frame_store_read("segments.out.0").num_rows == 2
        assert hg.frame_store_read("segments.out.1").num_rows == 2
        assert hg.frame_store_read("segments.out.2").num_rows == 1
        assert hg.frame_store_contains("segments.out.complete")

        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.REPLAY):
            assert hg.eval_node(rep) == [1, 2, 3, 4, 5]


def test_a_call_selects_its_backend_independently_of_the_graph():
    """``requires_`` runs before the node exists, so an overload cannot read
    node state to pick a backend - but it can read a scalar wiring argument.
    ``model`` is that argument: the graph here is configured for the in-memory
    model, and only the DataFrame overload writes to the frame store."""
    @hg.graph
    def g(ts: hg.TSD[str, hg.TS[float]]):
        hg.record(ts, key="out", recordable_id="local", model="DataFrame")

    with hg.GlobalState():
        hg.set_record_replay_model("InMemory")
        with hg.RecordReplayContext(mode=hg.RecordReplayEnum.RECORD):
            hg.eval_node(g, [{"a": 1.0}])

        frame = hg.frame_store_read("local.out")

    # A frame exists at all only because the call chose the DataFrame backend.
    assert _rows(frame) == 1
    assert _columns(frame) == ["__date_time__", "__as_of__", "__key_1__", "value"]
