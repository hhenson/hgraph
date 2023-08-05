from hg import graph, run


@graph
def main_graph():
    c = const("World")
    log("Hello", c)

def main():
    run(main_graph)
