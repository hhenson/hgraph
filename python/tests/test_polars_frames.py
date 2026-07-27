"""Pin issue #80: the polars_frames compatibility switch.

When enabled (feature flag ``polars_frames`` — ``HGRAPH_POLARS_FRAMES=true``
or the hgraph_features config file), outbound Frame/Series values surface as
``polars.DataFrame``/``polars.Series`` instead of ``pyarrow.Table``/``Array``.
Inbound polars is accepted regardless of the switch (anything exposing
``__arrow_c_stream__`` converts on ingest). Arrow remains the canonical
runtime substrate; the switch is a Python-boundary veneer only.
"""

import sys
from dataclasses import dataclass

import pyarrow as pa
import pytest

import _hgraph
from hgraph import CompoundScalar, Frame, Series, TS, pass_through
from hgraph.test import eval_node

polars = pytest.importorskip("polars")


@dataclass(frozen=True)
class PriceRow(CompoundScalar):
    instrument: str
    value: float


@pytest.fixture
def polars_frames():
    _hgraph.set_polars_frames(True)
    try:
        yield
    finally:
        _hgraph.set_polars_frames(False)


def _price_table():
    return pa.table({"instrument": ["A", "B"], "value": [101.5, 7.25]})


def test_switch_defaults_off_and_frames_stay_pyarrow():
    assert _hgraph.polars_frames() is False
    result = eval_node(
        pass_through,
        [_price_table()],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, pa.Table)


def test_frames_surface_as_polars_dataframes(polars_frames):
    result = eval_node(
        pass_through,
        [_price_table()],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, polars.DataFrame)
    assert result.equals(polars.DataFrame(
        {"instrument": ["A", "B"], "value": [101.5, 7.25]}))


def test_polars_dataframes_accepted_inbound_and_round_trip(polars_frames):
    # Inbound polars always converts (arrow C stream); with the switch on
    # the same object shape comes back out.
    frame = polars.DataFrame({"instrument": ["A"], "value": [1.5]})
    result = eval_node(
        pass_through,
        [frame],
        resolution_dict={"ts": TS[Frame[PriceRow]]},
    )[0]
    assert isinstance(result, polars.DataFrame)
    assert result.equals(frame)


def test_series_surface_as_polars_series(polars_frames):
    result = eval_node(
        pass_through,
        [pa.array([1.0, 2.5])],
        resolution_dict={"ts": TS[Series[float]]},
    )[0]
    assert isinstance(result, polars.Series)
    assert result.to_list() == [1.0, 2.5]


def test_shipped_frame_consumers_survive_the_switch(polars_frames):
    # Shipped python nodes keep their pyarrow internals: inputs normalize
    # through as_arrow_table, so a polars-surfaced frame still filters with
    # a pyarrow.compute expression (review finding on issue #80).
    import pyarrow.compute as pc
    from hgraph import TSB, TS_SCHEMA  # noqa: F401  (adaptor import needs hgraph loaded)
    from hgraph.adaptors.data_frame import filter_exp

    from hgraph import graph

    @graph
    def filtered(ts: TS[Frame[PriceRow]]) -> TS[Frame[PriceRow]]:
        return filter_exp(ts, pc.field("value") > 10.0)

    result = eval_node(filtered, [_price_table()])[0]
    assert isinstance(result, polars.DataFrame)
    assert result["instrument"].to_list() == ["A"]


def test_frame_store_read_presents_naive_utc_timestamps():
    # The v2 table codec stamps engine-time columns timestamp[us, UTC];
    # the user boundary presents them NAIVE (upstream parity) and drops the
    # version marker so the frame re-ingests as the v1 form.
    from datetime import datetime

    import hgraph as hg
    from hgraph import graph
    from hgraph.test import eval_node

    hg.set_record_replay_config(hg.DATA_FRAME)
    try:
        @hg.component
        def snap(x: TS[int]) -> TS[int]:
            return x + x

        @graph
        def recording(x: TS[int]) -> TS[int]:
            with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
                return snap(x)

        eval_node(recording, [1, 2, 3])
        table = hg.frame_store_read("snap.__out__")
        assert isinstance(table, pa.Table)
        for field in table.schema:
            if pa.types.is_timestamp(field.type):
                assert field.type.tz is None, field
        first = table.to_pylist()[0]["__date_time__"]
        assert isinstance(first, datetime) and first.tzinfo is None
        assert b"hgraph.temporal.version" not in (table.schema.metadata or {})
    finally:
        hg.set_record_replay_config(hg.IN_MEMORY)


def test_strip_presents_engine_columns_naive_even_in_zoned_frames():
    # A frame carrying ZonedDateTime values (a STRUCT column + the
    # hgraph.tzdb.version marker) still presents its top-level engine
    # timestamp columns naive; the zoned struct's nested timestamp and the
    # tzdb marker are untouched, and the temporal marker drops (v1 form).
    from datetime import datetime, timezone

    from hgraph._frame import _strip_utc_timestamps

    zoned_type = pa.struct([
        pa.field("instant", pa.timestamp("us", tz="UTC")),
        pa.field("zone", pa.string()),
        pa.field("offset_seconds", pa.int32()),
    ])
    when = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
    table = pa.table(
        {
            "__date_time__": pa.array([when], pa.timestamp("us", tz="UTC")),
            "value": pa.array(
                [{"instant": when, "zone": "Europe/London", "offset_seconds": 3600}],
                zoned_type,
            ),
        },
    ).replace_schema_metadata({
        b"hgraph.temporal.version": b"2",
        b"hgraph.tzdb.version": b"2026a",
    })

    stripped = _strip_utc_timestamps(table)
    assert stripped.schema.field("__date_time__").type == pa.timestamp("us")
    assert stripped.schema.field("value").type == zoned_type
    metadata = stripped.schema.metadata
    assert b"hgraph.temporal.version" not in metadata
    assert metadata[b"hgraph.tzdb.version"] == b"2026a"


def test_data_frame_storage_read_presents_user_form(polars_frames):
    # RecorderAPI/DataFrameStorage reads are a user-facing framework
    # surface: with the switch on the user reads polars, while internal
    # replay keeps working (it normalizes back to Arrow).
    from datetime import datetime

    import hgraph as hg
    from hgraph import GlobalState, MIN_ST, MIN_TD, record, replay, set_as_of, set_record_replay_model
    from hgraph.adaptors.data_frame import DATA_FRAME_RECORD_REPLAY, MemoryDataFrameStorage
    from hgraph.test import eval_node

    with GlobalState(), MemoryDataFrameStorage() as storage:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")

        frame = storage.read_frame("test.ts")
        assert isinstance(frame, polars.DataFrame)
        assert frame["value"].to_list() == [1, 2, 3]
        assert frame["__date_time__"].dtype.time_zone is None

        # Internal replay consumes the same storage unaffected.
        assert eval_node(replay[TS[int]], key="ts", recordable_id="test") == [1, 2, 3]


def test_enabled_without_polars_raises_clearly(polars_frames):
    saved = {name: module for name, module in sys.modules.items()
             if name == "polars" or name.startswith("polars.")}
    for name in saved:
        sys.modules[name] = None
    try:
        with pytest.raises(Exception, match="polars is not installed"):
            eval_node(
                pass_through,
                [_price_table()],
                resolution_dict={"ts": TS[Frame[PriceRow]]},
            )
    finally:
        sys.modules.update(saved)
