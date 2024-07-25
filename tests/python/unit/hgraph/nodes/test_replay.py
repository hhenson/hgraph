from hgraph import graph, TS, run_graph, print_, replay
from hgraph._impl._operators._record_replay_in_memory import SimpleArrayReplaySource, set_replay_values


def test_replay_simple():

    set_replay_values("test", SimpleArrayReplaySource(["1", "2", "3"]))

    @graph
    def main():
        values = replay("test", TS[str])
        print_(values)

    run_graph(main, __trace__=True)
