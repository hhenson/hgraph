from pathlib import Path

from hgraph import graph, MIN_TD
from hgraph._impl._persistence._graph_persistence_builder import suspender, RestoreGraphObserver
from hgraph.nodes import const, debug_print, default
from hgraph.test import eval_node


@graph
def simple_graph():
    s = const(True, delay=MIN_TD*2)
    c1 = default(const(3, delay=MIN_TD*2), 1)
    c2 = const(2)
    a = c1 + c2
    debug_print("test", a)
    suspender(s, Path.home().joinpath("tmp/test_suspend_resume"), "suspend")


def test_suspend_resume():
    eval_node(simple_graph)
    print("Restoring graph")
    eval_node(simple_graph,
              __observers__=[RestoreGraphObserver(Path.home().joinpath("tmp/test_suspend_resume"), "suspend")])
