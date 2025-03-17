from hgraph import GlobalState, set_record_replay_model, record, TS, set_as_of, MIN_ST, MIN_TD, replay, replay_const
from hgraph.adaptors.data_frame import DATA_FRAME_RECORD_REPLAY, MemoryDataFrameStorage, replay_data_frame
from hgraph.test import eval_node


def test_data_frame_record():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")

        assert len(ds._frames) == 1
        df = next(iter(ds._frames.values()))
        assert len(df) == 3
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
        data_frame = ds.read_frame("test::ts")
        assert eval_node(replay_data_frame[TS[int]], data_frame) == [1, 2, 3]


def test_data_frame_record_replay_const():
    with GlobalState() as gs, MemoryDataFrameStorage() as ds:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        eval_node(record[TS[int]], ts=[1, 2, 3], key="ts", recordable_id="test")
        assert eval_node(replay_const[TS[int]], key="ts", recordable_id="test", __start_time__=MIN_ST + MIN_TD) == [
            2,
        ]
