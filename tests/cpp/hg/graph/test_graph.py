from hgraph import graph, run_graph
from hgraph.nodes import const, write


@graph
def main_graph():
    c = const("World")
    write("Hello", c)

def main():
    run_graph(main_graph)
