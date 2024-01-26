from typing import Type

from hgraph import compute_node, SCALAR, TS, NoDups, NODUPS_FLAG, graph, AUTO_RESOLVE, AllowDups
from hgraph.test import eval_node


def test_dedup():
    @compute_node
    def drop_dups(ts: TS[int]) -> TS[int, NoDups]:
        return ts.value

    assert (eval_node(drop_dups, [1, 2, 2, None, 3, None, 3, 4]) ==
            [1, 2, None, None, 3, None, None, 4])


def test_dedup_generic():
    @compute_node
    def drop_dups(ts: TS[SCALAR]) -> TS[SCALAR, NoDups]:
        return ts.value

    assert (eval_node(drop_dups, [1, 2, 2, None, 3, None, 3, 4]) ==
            [1, 2, None, None, 3, None, None, 4])


def test_dedup_overload_on_flag():
    @compute_node
    def gen_dups(ts: TS[SCALAR]) -> TS[SCALAR]:
        return ts.value

    @compute_node
    def gen_no_dups(ts: TS[SCALAR]) -> TS[SCALAR, NoDups]:
        return ts.value

    @compute_node
    def drop_dups(ts: TS[SCALAR]) -> TS[SCALAR, NoDups]:
        return ts.value + 1

    @graph(overloads=drop_dups)
    def drop_dups_skip(ts: TS[SCALAR, NoDups]) -> TS[SCALAR]:
        return ts

    @graph
    def g_1(ts: TS[SCALAR]) -> TS[SCALAR]:
        return drop_dups(gen_dups(ts))

    @graph
    def g_2(ts: TS[SCALAR]) -> TS[SCALAR]:
        return drop_dups(gen_no_dups(ts))

    assert (eval_node(g_1, [1, 2, 2, None, 3, None, 3, 4], __trace__=True) ==
            [2, 3, None, None, 4, None, None, 5])

    assert (eval_node(g_2, [1, 2, 2, None, 3, None, 3, 4]) ==
            [1, 2, None, None, 3, None, None, 4])


def test_dedup_typevar():
    @compute_node
    def gen_dups(ts: TS[SCALAR]) -> TS[SCALAR]:
        return ts.value

    @compute_node
    def gen_no_dups(ts: TS[SCALAR]) -> TS[SCALAR, NoDups]:
        return ts.value

    @graph
    def drop_dups_resolve(ts: TS[SCALAR, NODUPS_FLAG], has_dups: Type[NODUPS_FLAG] = AUTO_RESOLVE) -> TS[SCALAR, NODUPS_FLAG]:
        return ts + 1 if has_dups == AllowDups else ts

    @graph
    def g_1(ts: TS[SCALAR]) -> TS[SCALAR]:
        return drop_dups_resolve(gen_dups(ts))

    @graph
    def g_2(ts: TS[SCALAR]) -> TS[SCALAR]:
        return drop_dups_resolve(gen_no_dups(ts))

    assert (eval_node(g_1, [1, 2, 2, None, 3, None, 3, 4]) ==
            [2, 3, 3, None, 4, None, 4, 5])

    assert (eval_node(g_2, [1, 2, 2, None, 3, None, 3, 4]) ==
            [1, 2, None, None, 3, None, None, 4])
