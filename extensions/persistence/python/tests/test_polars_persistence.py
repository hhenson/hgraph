"""Polars presentation of the durable storage surfaces (moved with
hgraph-persistence, RFC 0025 checkpoint 4).

The polars_frames switch itself (issue #80) is pinned by core's
``test_polars_frames``; this file covers only the durable-store reads.
"""

import pyarrow as pa
import pytest

import _hgraph
from hgraph import TS

polars = pytest.importorskip("polars")


@pytest.fixture
def polars_frames():
    previous = _hgraph.polars_frames()
    _hgraph.set_polars_frames(True)
    try:
        yield
    finally:
        _hgraph.set_polars_frames(previous)


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


