from typing import Tuple, Callable


from hgraph import (
    TS,
    TSD,
    switch_,
    graph,
    pass_through,
    mesh_,
    contains_,
    TSS,
    TSL,
    Removed,
    REMOVE,
    DEFAULT,
    combine,
    match_,
    convert,
    const,
)
from hgraph._operators._flow_control import merge
from hgraph.test import eval_node


def test_mesh():
    @graph
    def get_arg(name: TS[str], vars: TSD[str, TS[float]]) -> TS[float]:
        return switch_(
            {True: lambda n, v: v[n], False: lambda n, v: mesh_(operation)[n]}, contains_(vars, name), n=name, v=vars
        )

    @graph
    def perform_op(op_name: TS[str], lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return switch_(
            {"+": lambda l, r: l + r, "-": lambda l, r: l - r, "*": lambda l, r: l * r, "/": lambda l, r: l / r},
            op_name,
            lhs,
            rhs,
        )

    @graph
    def operation(i: TS[Tuple[str, ...]], vars: TSD[str, TS[float]]) -> TS[float]:
        return perform_op(i[0], get_arg(i[1], vars), get_arg(i[2], vars))

    @graph
    def g(i: TSD[str, TS[Tuple[str, ...]]], vars: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return mesh_(operation, i, pass_through(vars))

    assert eval_node(
        g,
        i=[{"c": ("+", "a", "b"), "d": ("-", "c", "x")}, {"e": ("*", "d", "a")}],
        vars=[{"a": 1.0, "b": 2.0, "x": 3.0}, None, None, {"a": 2.0}],
    ) == [{"c": 3.0}, {"d": 0.0}, {"e": 0.0}, {"c": 4.0, "d": 1.0, "e": 2.0}]


def test_mesh_2():
    @graph
    def perform_op(op_name: TS[str], lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return switch_(
            {"+": lambda l, r: l + r, "-": lambda l, r: l - r, "*": lambda l, r: l * r, "/": lambda l, r: l / r},
            op_name,
            lhs,
            rhs,
        )

    @graph
    def operation(i: TS[str], vars: TS[str]) -> TS[float]:
        what = vars
        number = match_("^([0-9]+(?:\.[0-9]*)?)$", what)
        var = match_("^(\w+)$", what)
        expr = match_("^(\w+)([+\-*/])(\w+)$", what)
        return switch_(
            {
                (True, False, False): lambda n: convert[TS[float]](n[0]),
                (False, True, False): lambda n: mesh_(operation)[n[0]],
                (False, False, True): lambda n: perform_op(n[1], mesh_(operation)[n[0]], mesh_(operation)[n[2]]),
            },
            combine[TS[Tuple[bool, bool, bool]]](number.is_match, var.is_match, expr.is_match),
            n=merge(number.groups, var.groups, expr.groups),
        )

    @graph
    def g(i: TSS[str], vars: TSD[str, TS[str]]) -> TSD[str, TS[float]]:
        return mesh_(operation, __key_arg__="i", __keys__=i, vars=vars)

    r = eval_node(
        g,
        __trace__={"start": False, "stop": False},
        i=[None, "e"] + [None] * 20 + ["f", "c"] + [None] * 10 + [{Removed("e")}],
        vars=[{"a": "1.", "b": "2.", "c": "a+b", "d": "c-x", "x": "3.", "e": "d*a"}]
        + [None] * 10
        + [{"a": "2."}]
        + [None] * 10
        + [{"f": "b+x"}]
        + [None] * 12
        + [{"b": "1."}],
    )

    assert [x for x in r if x] == [{"e": 0.0}, {"e": 2.0}, {"f": 5.0}, {"c": 4.0}, {"e": REMOVE}, {"f": 4.0, "c": 3.0}]


def test_mesh_named():
    @graph
    def fib(n: TS[int]) -> TS[int]:
        return switch_(
            {
                0: lambda key: const(0),
                1: lambda key: const(1),
                DEFAULT: lambda key: mesh_("fib")[key - 1] + mesh_("fib")[key - 2],
            },
            n,
        )

    @graph
    def g(i: TSS[int]) -> TSD[int, TS[int]]:
        return mesh_(fib, __key_arg__="n", __keys__=i, __name__="fib")

    assert eval_node(g, [{7}, {8}, {9}], __trace__=True)[-1] == {7: 13, 8: 21, 9: 34}
