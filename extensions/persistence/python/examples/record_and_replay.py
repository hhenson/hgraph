"""Record a scalar stream, inspect its Arrow frame, and replay it."""

import hgraph_persistence as persistence

import hgraph as hg


@hg.graph
def record_values(value: hg.TS[int]) -> None:
    hg.record(value, key="values", recordable_id="example")


@hg.graph
def replay_values() -> hg.TS[int]:
    return hg.replay[hg.TS[int]](key="values", recordable_id="example")


def run_example():
    with hg.GlobalState():
        hg.set_record_replay_config(persistence.FRAME_BACKEND)
        hg.eval_node(record_values, [10, 20, 30])

        frame = persistence.frame_store_read("example.values")
        replayed = hg.eval_node(replay_values)
        return frame, replayed


if __name__ == "__main__":
    stored, values = run_example()
    print(stored)
    print("replayed:", values)
