"""The DATA_FRAME replay start-time filter must apply on EVERY run entry
point. ``__start_time__`` (the Python-readable mirror of the run start) was
only published by ``eval_node``, so on the ``evaluate_graph``/``run_graph``
path the replay overloads' pre-start filter was inert: a stored frame
carrying rows dated before ``start_time`` fed them to the native replay
generator and the whole stream came out empty. The sibling of the
wiring/GlobalState seeding fix (d809ff63) — same subsystem, different entry
point.
"""

import hgraph as hg
from hgraph import MIN_ST, MIN_TD, TS, TSD
from hgraph.adaptors.data_frame import (
    DATA_FRAME_RECORD_REPLAY,
    MemoryDataFrameStorage,
)
from hgraph.test import eval_node


def _record(ts_type, key, values):
    hg.set_record_replay_model(DATA_FRAME_RECORD_REPLAY)
    hg.set_as_of(MIN_ST + MIN_TD * 30)
    eval_node(hg.record[ts_type], ts=values, key=key, recordable_id="test")


def test_data_frame_replay_drops_pre_start_rows_via_evaluate_graph():
    with hg.GlobalState(), MemoryDataFrameStorage():
        _record(TS[float], "ts", [1.0, 2.0, 3.0])

        @hg.graph
        def replay_g() -> TS[float]:
            return hg.replay[TS[float]](key="ts", recordable_id="test")

        # The first two recorded rows predate the run: they must be DROPPED
        # (not crash the stream into emptiness).
        start = MIN_ST + 2 * MIN_TD
        out = hg.evaluate_graph(replay_g, hg.GraphConfiguration(start_time=start))
        assert [value for _, value in out] == [3.0]
        assert [when for when, _ in out] == [start]


def test_data_frame_replay_keyed_history_before_start_via_evaluate_graph():
    # The reported shape: a keyed series backfilled with history long before
    # the run start, replayed as TSD[str, TS[float]].
    with hg.GlobalState(), MemoryDataFrameStorage():
        _record(TSD[str, TS[float]], "prices",
                [{"a": 1.0}, {"a": 2.0}, {"a": 3.0}, {"a": 4.0}])

        @hg.graph
        def replay_g() -> TSD[str, TS[float]]:
            return hg.replay[TSD[str, TS[float]]](key="prices", recordable_id="test")

        start = MIN_ST + 2 * MIN_TD
        out = hg.evaluate_graph(replay_g, hg.GraphConfiguration(start_time=start))
        # evaluate_graph's sparse recording presents the canonical TSD delta
        # (removed/modified/removed_strict), not eval_node's friendly dict.
        assert [value["modified"] for _, value in out] == [{"a": 3.0}, {"a": 4.0}]
        assert all(not value["removed"] for _, value in out)
