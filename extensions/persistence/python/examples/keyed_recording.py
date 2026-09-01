"""Persist and replay updates to a keyed time series."""

import hgraph_persistence as persistence

import hgraph as hg

Positions = hg.TSD[str, hg.TS[float]]


@hg.graph
def record_positions(positions: Positions) -> None:
    hg.record(positions, key="positions", recordable_id="risk")


@hg.graph
def replay_positions() -> Positions:
    return hg.replay[Positions](key="positions", recordable_id="risk")


def run_example():
    updates = [{"AAPL": 10.0}, {"MSFT": 5.0}, {"AAPL": 12.0}]
    with hg.GlobalState():
        hg.set_record_replay_config(persistence.FRAME_BACKEND)
        hg.eval_node(record_positions, updates)

        frame = persistence.frame_store_read("risk.positions")
        replayed = hg.eval_node(replay_positions)
        return frame, replayed


if __name__ == "__main__":
    stored, updates = run_example()
    print(stored)
    print("replayed:", updates)
