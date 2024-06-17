from hgraph import graph, TS, run_graph, MIN_ST, MIN_TD, print_
from hgraph.nodes import set_replay_values, SimpleArrayReplaySource, replay, record, get_recorded_value


def test_recorder():
    """
    Note, this test depends on the replay functionality to be valid.
    So bootstrap replay before validating the recorder.
    Once this is working the unit tester should become functional.
    """

    set_replay_values("test", SimpleArrayReplaySource(["1", "2", "3"]))

    @graph
    def main():
        value_ts = replay("test", TS[str])
        record(value_ts)
        print_(value_ts)

    run_graph(main)

    values = get_recorded_value()
    assert values == [(MIN_ST, "1"), (MIN_ST+MIN_TD, "2"), (MIN_ST+2*MIN_TD, "3")]