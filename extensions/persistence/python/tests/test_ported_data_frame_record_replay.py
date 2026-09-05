"""Ported from hgraph_unit_tests/adaptors/data_frame/test_data_frame_record_replay.py.

Changes from upstream:
- Arrow-boundary ruling (2026-07-17): the Frame substrate is Arrow, so stored
  frames read back as ``pyarrow.Table``. Assertions written against polars
  DataFrames (``df["value"][0] == 1``, ``[k for k in df.schema]`` yielding
  column NAMES) convert at the boundary with ``pl.from_arrow``; everything
  else is upstream-verbatim (the record/replay/replay_const round-trips and
  the TSD dict comparisons run unchanged).
"""
import polars as pl

from hgraph import GlobalState, set_record_replay_model, record, TS, set_as_of, MIN_ST, MIN_TD, replay, replay_const, \
    TSD, set_table_schema_date_key
from hgraph.adaptors.data_frame import DATA_FRAME_RECORD_REPLAY, MemoryDataFrameStorage, replay_data_frame, \
    set_data_frame_overrides
from hgraph.test import eval_node


def _stored_frame(ds):
    assert len(ds._frames) == 1
    return pl.from_arrow(next(iter(ds._frames.values())))


def test_data_frame_record():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        fixed_as_of = MIN_ST + MIN_TD * 30
        set_as_of(fixed_as_of)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")

        df = _stored_frame(ds)
        assert len(df) == 3
        assert df["__as_of__"].to_list() == [fixed_as_of] * 3
        assert df["value"][0] == 1
        assert df["value"][1] == 2
        assert df["value"][2] == 3


def test_data_frame_record_replay():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")
        assert len(ds._frames) == 1
        assert eval_node(replay[TS[int]], key="ts", recordable_id="test") == [1, 2, 3]
        data_frame = ds.read_frame("test.ts")
        assert eval_node(replay_data_frame[TS[int]], data_frame) == [1, 2, 3]


def test_data_frame_record_replay_const():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")
        assert eval_node(replay_const[TS[int]], key="ts", recordable_id="test", __start_time__=MIN_ST + MIN_TD) == [
            2,
        ]


def test_data_frame_record_replay_overrides_ts():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        set_data_frame_overrides(key="ts", recordable_id="test", track_as_of=False)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")

        df = _stored_frame(ds)
        assert len(df.schema) == 2

        assert eval_node(replay[TS[int]], key="ts", recordable_id="test") == [1, 2, 3]


def test_data_frame_record_replay_overrides_accept_keyword_recordable_id():
    # ``replay(key, tp=AUTO_RESOLVE, recordable_id=None, ...)`` is the 0.5
    # signature (RFC 0033): ``tp`` sits at position 1, so ``recordable_id`` is
    # passed by keyword after the subscript selects the type.
    with GlobalState(), MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        set_table_schema_date_key("date")
        set_data_frame_overrides(
            key="ts",
            recordable_id="test",
            track_as_of=False,
            track_removes=False,
            partition_keys=["id"],
        )

        eval_node(record[TSD[str, TS[int]]], [{"a": 1}, {"b": 2}, {"a": 3}], "ts", "test")

        assert list(_stored_frame(ds).schema) == ["date", "id", "value"]
        assert eval_node(replay[TSD[str, TS[int]]], "ts", recordable_id="test") == [
            {"a": 1},
            {"b": 2},
            {"a": 3},
        ]


def test_data_frame_record_replay_overrides_tsd():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        set_table_schema_date_key("date")
        set_data_frame_overrides(key="ts", recordable_id="test", track_as_of=False, track_removes=False,
                                 partition_keys=["id"])
        eval_node(record[TSD[str, TS[int]]], ts=[{"a": 1}, {"b": 2}, {"a": 3}], key="ts", recordable_id="test")

        df = _stored_frame(ds)
        assert len(df.schema) == 3
        assert [k for k in df.schema] == ["date", "id", "value"]

        assert eval_node(replay[TSD[str, TS[int]]], key="ts", recordable_id="test") == [{"a": 1}, {"b": 2}, {"a": 3}]


def test_data_frame_record_replay_overrides_tsd_wr():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        set_table_schema_date_key("date")
        set_data_frame_overrides(key="ts", recordable_id="test", track_as_of=False, partition_keys=["id"],
                                 remove_partition_keys=["id_removed"])
        eval_node(record[TSD[str, TS[int]]], ts=[{"a": 1}, {"b": 2}, {"a": 3}], key="ts", recordable_id="test")

        df = _stored_frame(ds)
        assert len(df.schema) == 4
        assert [k for k in df.schema] == ["date", "id_removed", "id", "value"]

        assert eval_node(replay[TSD[str, TS[int]]], key="ts", recordable_id="test") == [{"a": 1}, {"b": 2}, {"a": 3}]
