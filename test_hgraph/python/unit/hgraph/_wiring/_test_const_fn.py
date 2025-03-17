from hgraph import const_fn, TS, graph, const, TIME_SERIES_TYPE, operator
from hgraph.test import eval_node


def test_const_fn_as_ts():

    @const_fn
    def my_const_fn(a: int, b: int) -> TS[int]:
        return a + b

    assert eval_node(my_const_fn, 1, 2) == [3]


def test_const_fn_as_scalar():
    @const_fn
    def my_const_fn(a: int, b: int) -> TS[int]:
        return a + b

    assert my_const_fn(1, 2) == 3


def test_const_fn_value_in_graph():

    @const_fn
    def my_const_fn(a: int, b: int) -> TS[int]:
        return a + b

    @graph
    def my_graph() -> TS[bool]:
        if my_const_fn(1, 2).value == 3:
            return const(True)
        else:
            return const(False)

    assert eval_node(my_graph) == [True]


def test_const_fn_resolution():

    @operator
    def my_const_operator(tp: type[TIME_SERIES_TYPE]) -> TS[str]: ...

    @const_fn(overloads=my_const_operator)
    def my_const_fn(tp: type[TS[int]]) -> TS[str]:
        return "int"

    @const_fn(overloads=my_const_operator)
    def my_const_fn(tp: type[TS[float]]) -> TS[str]:
        return "float"

    assert my_const_operator(TS[int]) == "int"
