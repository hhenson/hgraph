"""
Ability to create breakpoints in the graph.
"""

from hgraph import compute_node, TSB, TS_SCHEMA, graph, const, debug_print, operator, OUT, TS

__all__ = ("breakpoint_",)


@operator
def breakpoint_(ts: OUT) -> OUT:
    """
    Place a breakpoint in the graph.
    There are three key types of breakpoints supported:

    * breakpoint_(ts) - Breaks when the ts value is modified.
    * breakpoint_(condition, ts) - Breaks when the condition is True and either the condition or the value is modified.
    * breakpoint_(**kwargs) - Breaks when any of the inputs are modified.
    """


@compute_node(overloads=breakpoint_)
def breakpoint_ts(ts: OUT) -> OUT:
    breakpoint()
    return ts.delta_value


@compute_node(overloads=breakpoint_, valid=("ts",))
def breakpoint_conditional(condition: TS[bool], ts: OUT) -> OUT:
    if condition.valid and condition.value:
        breakpoint()
    return ts.delta_value


@compute_node(overloads=breakpoint_, valid=tuple())
def breakpoint_many(**kwargs: TSB[TS_SCHEMA]) -> TSB[TS_SCHEMA]:
    breakpoint()
    return kwargs.delta_value


if __name__ == "__main__":
    from hgraph.test import eval_node

    @graph
    def breakpoint_graph():
        c = const(1)
        debug_print("value", breakpoint_(c))

    eval_node(breakpoint_graph)

    @graph
    def breakpoint_conditional_graph(condition: TS[bool], value: TS[int]):
        debug_print("value", breakpoint_(condition, value))

    eval_node(breakpoint_conditional_graph, [None, False, True], value=[1, 2, 3])

    @graph
    def breakpoint_many_graph(a: TS[int], b: TS[int], c: TS[int]):
        a, b, c = breakpoint_many(a=a, b=b, c=c)
        debug_print("a", a)
        debug_print("b", b)
        debug_print("c", c)

    eval_node(breakpoint_many_graph, [1, None, 3], [None, 4, None], [None, None, 6])
