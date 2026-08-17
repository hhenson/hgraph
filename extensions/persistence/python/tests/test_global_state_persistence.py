"""Wiring-time backend selection with the durable storage surface
(moved with hgraph-persistence, RFC 0025 checkpoint 4)."""

"""Pin the GlobalState wiring lifecycle (ruling 2026-07-27).

The wiring phase reads the LIVE selected GlobalState — the same store the
configuration setters (set_record_replay_model, set_as_of, ...) write — so
configuration set during wiring, including inside a graph function, is
honoured. The run takes its isolation copy at graph build; results copy back
at run end. A construction-time copy previously split the two stores and
silently ignored in-graph configuration (a client's replay defaulted to
IN_MEMORY). Nested GlobalState activation remains a loud modeling error.
"""

import pyarrow as pa
import pytest

import hgraph as hg
from hgraph import GlobalState, TS, set_as_of, set_record_replay_model
from hgraph.adaptors.data_frame import (
    DATA_FRAME_RECORD_REPLAY,
    MemoryDataFrameStorage,
)
from hgraph.test import eval_node


def _frame(values):
    stamps = [hg.MIN_ST + i * hg.MIN_TD for i in range(len(values))]
    return pa.table({
        "__date_time__": stamps,
        "__as_of__": stamps,
        "value": values,
    })


def test_record_replay_model_set_inside_the_graph_is_honoured():
    # The client scenario: configuration applied DURING WIRING, inside the
    # graph function, on the selected state — no nested GlobalState.
    @hg.graph
    def wired(ts: TS[int]) -> TS[int]:
        set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
        set_as_of(hg.MIN_ST + hg.MIN_TD * 30)
        return hg.replay[TS[int]](key="ts", recordable_id="client")

    with GlobalState(), MemoryDataFrameStorage() as storage:
        storage.write_frame("client.ts", _frame([7, 8]))
        assert eval_node(wired, [None, None]) == [7, 8]


def test_record_replay_model_set_in_graph_with_implicit_state():
    with MemoryDataFrameStorage() as storage:
        storage.write_frame("implicit.ts", _frame([42]))

        @hg.graph
        def wired(ts: TS[int]) -> TS[int]:
            set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
            set_as_of(hg.MIN_ST + hg.MIN_TD * 30)
            return hg.replay[TS[int]](key="ts", recordable_id="implicit")

        assert eval_node(wired, [None]) == [42]


