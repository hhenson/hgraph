from hgraph import graph, TS, run_graph
from hgraph.nodes import set_replay_values, SimpleArrayReplaySource, replay, print_


def test_replay_simple():

    set_replay_values("test", SimpleArrayReplaySource(["1", "2", "3"]))

    @graph
    def main():
        values = replay("test", TS[str])
        print_(values)

    run_graph(main, __trace__=True)
