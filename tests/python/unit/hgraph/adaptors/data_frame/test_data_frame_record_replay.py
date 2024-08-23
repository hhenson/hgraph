from unittest.mock import patch

import pytest

from hgraph import GlobalState, set_record_replay_model, record, TS, set_as_of, MIN_ST, MIN_TD, replay
from hgraph.adaptors.data_frame import DATA_FRAME_RECORD_REPLAY
from hgraph.test import eval_node


def test_data_frame_record():
    with GlobalState() as gs:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        with patch("hgraph.adaptors.data_frame._data_frame_record_replay._write_df") as write_df:
            eval_node(record[TS[int]], ts=[1, 2, 3], key="ts")
            write_df.assert_called_once()
            df = write_df.call_args[0][0]
            assert len(df) == 3
            assert df["value"][0] == 1
            assert df["value"][1] == 2
            assert df["value"][2] == 3


def test_data_frame_record_replay():
    with GlobalState() as gs:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(MIN_ST + MIN_TD * 30)
        with patch("hgraph.adaptors.data_frame._data_frame_record_replay._write_df") as write_df:
            eval_node(record[TS[int]], ts=[1, 2, 3], key="ts")
            write_df.assert_called_once()
            df = write_df.call_args[0][0]
        with patch("hgraph.adaptors.data_frame._data_frame_record_replay._read_df") as read_df:
            read_df.return_value = df
            assert eval_node(replay[TS[int]], key="ts") == [1, 2, 3]
