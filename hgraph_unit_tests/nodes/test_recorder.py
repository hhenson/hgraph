from hgraph import graph, TS, MIN_ST, MIN_TD, print_, replay, record, GlobalState, evaluate_graph, \
    GraphConfiguration
from hgraph._impl._operators._record_replay_in_memory import (
    SimpleArrayReplaySource,
    set_replay_values,
    get_recorded_value,
)


def test_recorder():
    """
    Note, this test depends on the replay functionality to be valid.
    So bootstrap replay before validating the recorder.
    Once this is working the unit tester should become functional.
    """

    with GlobalState():
        set_replay_values("test", SimpleArrayReplaySource(["1", "2", "3"]))

        @graph
        def main():
            value_ts = replay("test", TS[str])
            record(value_ts)
            print_(value_ts)

        evaluate_graph(main, GraphConfiguration())

        values = get_recorded_value()
        assert values == [(MIN_ST, "1"), (MIN_ST + MIN_TD, "2"), (MIN_ST + 2 * MIN_TD, "3")]
