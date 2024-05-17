from hgraph import TS, graph, TSD, map_, sink_node, TIME_SERIES_TYPE, pass_through, reduce, compute_node, TSL, Size, \
    switch_
from hgraph.nodes import add_ts, sub_ts
from hgraph.test import eval_node

# Map


@compute_node
def convert(ts: TS[int]) -> TS[str]:
    """Convert the input to a time series."""
    return str(ts.value)


@graph
def graph_tsd(tsd: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
    return map_(convert, tsd)


print(eval_node(graph_tsd, tsd=[{"a": 1, "b": 6}, {"a": 2, "b": 7}]))


@graph
def graph_tsl(tsl: TSL[TS[int], Size[2]]) -> TSL[TS[str], Size[2]]:
    return map_(convert, tsl)


print(eval_node(graph_tsl, tsl=[{0: 1, 1: 6}, {0: 2, 1: 7}]))


@sink_node
def print_input(key: TS[str], ts: TIME_SERIES_TYPE, mode: str):
    print(f"[{mode}] {key.value}: {ts.delta_value}")


@graph
def graph_undecided(tsd: TSD[str, TS[int]]):
    map_(print_input, tsd, "No Passthrough")
    map_(print_input, pass_through(tsd), "Passthrough", __keys__=tsd.key_set)


print(eval_node(graph_undecided, tsd=[{"a": 1, "b": 6}, {"a": 2, "b": 7}]))

# Reduce


@graph
def graph_reduce_tsd(tsd: TSD[str, TS[int]]) -> TS[int]:
    return reduce(add_ts, tsd, 0)


print(eval_node(graph_reduce_tsd, tsd=[{"a": 1, "b": 6}, {"a": 2, "b": 7}]))


# Switch

@graph
def graph_switch(selector: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return switch_({
        "add": add_ts,
        "sub": sub_ts,
    }, selector, lhs, rhs)


print(eval_node(graph_switch, selector=["add", None, "sub", None], lhs=[1, 2, 3, 4], rhs=[2, 3, 4, 5]))


@graph
def graph_switch_lambda(selector: TS[str], lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return switch_({
        "add": lambda lhs, rhs: lhs + rhs,
        "sub": lambda lhs, rhs: lhs - rhs,
    }, selector, lhs, rhs)


print(eval_node(graph_switch_lambda, selector=["add", None, "sub", None], lhs=[1, 2, 3, 4], rhs=[2, 3, 4, 5]))
