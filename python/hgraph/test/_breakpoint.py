"""Graph breakpoints (ported from hgraph.test._breakpoint — this one is
genuinely test-package API, ruling 2026-07-31)."""

from hgraph import TS, TS_SCHEMA, TSB, OUT, compute_node, operator

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
