from hg import graph, TS, run_graph
from hg.nodes import set_replay_values, SimpleArrayReplaySource, replay, write_str


def test_replay_simple():

    set_replay_values("test", SimpleArrayReplaySource(["1", "2", "3"]))

    @graph
    def main():
        values = replay("test", TS[str])
        write_str(values)

    run_graph(main)
