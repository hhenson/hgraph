from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pytest

from hgraph import CompoundScalar, Frame, Series, TS, WiringError, graph
from hgraph.test import eval_node


@dataclass(frozen=True)
class TimedRow(CompoundScalar):
    timestamp: datetime
    val: float


def test_frame_columns_support_attribute_and_item_access():
    first = datetime(2024, 1, 2, 3, 4, 5)
    second = datetime(2025, 6, 7, 8, 9, 10)
    frame = pa.table({"timestamp": [first, second], "val": [1.5, 2.5]})

    @graph
    def attribute(ts: TS[Frame[TimedRow]]) -> TS[Series[datetime]]:
        return ts.timestamp

    @graph
    def item(ts: TS[Frame[TimedRow]]) -> TS[Series[datetime]]:
        return ts["timestamp"]

    assert eval_node(attribute, [frame])[0].equals(frame["timestamp"].combine_chunks())
    assert eval_node(item, [frame])[0].equals(frame["timestamp"].combine_chunks())


def test_frame_rows_support_scalar_and_time_series_indices():
    first = datetime(2024, 1, 2, 3, 4, 5)
    second = datetime(2025, 6, 7, 8, 9, 10)
    frame = pa.table({"timestamp": [first, second], "val": [1.5, 2.5]})

    @graph
    def scalar_index(ts: TS[Frame[TimedRow]], index: int) -> TS[TimedRow]:
        return ts[index]

    @graph
    def dynamic_index(ts: TS[Frame[TimedRow]], index: TS[int]) -> TS[TimedRow]:
        return ts[index]

    assert eval_node(scalar_index, [frame], 0) == [TimedRow(timestamp=first, val=1.5)]
    assert eval_node(scalar_index, [frame], -1) == [TimedRow(timestamp=second, val=2.5)]
    assert eval_node(dynamic_index, [frame, frame], [0, 1]) == [
        TimedRow(timestamp=first, val=1.5),
        TimedRow(timestamp=second, val=2.5),
    ]

    with pytest.raises(Exception):
        eval_node(scalar_index, [frame], 2)


def test_missing_frame_column_fails_during_wiring():
    frame = pa.table({"timestamp": [datetime(2024, 1, 1)], "val": [1.5]})

    @graph
    def missing(ts: TS[Frame[TimedRow]]) -> TS[Series[float]]:
        return ts.missing

    with pytest.raises(WiringError):
        eval_node(missing, [frame])
