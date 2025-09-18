from hgraph import graph, TS, TIME_SERIES_TYPE, sink_node
from hgraph.test import eval_node


@graph
def trivial_graph(a: TS[int], c: TS[int]):
    display((a + 1) * c)


@sink_node
def display(ts: TIME_SERIES_TYPE):
    print(ts.value)


def main():
    eval_node(
        trivial_graph, [None, 2, None, 4, None], [None, None, 3, None, 6], __trace__={"start": False, "stop": False}
    )


if __name__ == "__main__":
    main()
