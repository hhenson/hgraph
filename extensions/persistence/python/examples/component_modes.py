"""Apply durable record/replay modes around an unchanged component."""

import hgraph_persistence as persistence

import hgraph as hg


@hg.component
def calculate_total(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return lhs + rhs


@hg.graph
def recording(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
        return calculate_total(lhs, rhs)


@hg.graph
def replaying(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    with hg.record_replay_scope(hg.RecordReplayEnum.REPLAY):
        return calculate_total(lhs, rhs)


@hg.graph
def comparing(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    with hg.record_replay_scope(hg.RecordReplayEnum.COMPARE):
        return calculate_total(lhs, rhs)


@hg.graph
def recovering(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    with hg.record_replay_scope(hg.RecordReplayEnum.RECOVER):
        return calculate_total(lhs, rhs)


def run_example():
    with hg.GlobalState():
        hg.set_record_replay_config(persistence.FRAME_BACKEND)

        recorded = hg.eval_node(recording, [1, None, 3], [10, 20, None])
        replayed = hg.eval_node(replaying, [100, 100, 100], [100, 100, 100])
        hg.eval_node(comparing, [100, 100, 100], [100, 100, 100])
        comparison = hg.comparison_summary("calculate_total.__compare__")
        recovered = hg.eval_node(recovering, [None, 100], [None, None])

        return {
            "recorded": recorded,
            "replayed": replayed,
            "comparison": comparison,
            "recovered": recovered,
        }


if __name__ == "__main__":
    for name, result in run_example().items():
        print(f"{name}: {result}")
