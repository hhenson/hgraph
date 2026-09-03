from dataclasses import dataclass

from hgraph import REMOVE, CompoundScalar, TS, TSD, graph, merge
from hgraph.test import eval_node


def test_fixed_tsl_realizes_polymorphic_tsd_value_storage():
    @dataclass(frozen=True)
    class Request(CompoundScalar, abstract=True):
        symbol: str

    @dataclass(frozen=True)
    class Add(Request):
        quantity: int

    @dataclass(frozen=True)
    class Remove(Request):
        reason: str

    @graph
    def app(
        lhs: TSD[str, TS[Request]], rhs: TSD[str, TS[Request]]
    ) -> TSD[str, TS[Request]]:
        return merge(lhs, rhs, disjoint=True)

    add = Add(symbol="ADD", quantity=2)
    remove = Remove(symbol="REMOVE", reason="expired")
    assert eval_node(app, [{"add": add}], [{"remove": remove}]) == [
        {"add": add, "remove": remove}
    ]


def test_disjoint_merge_adapts_a_derived_tsd_input_to_the_base_element_type():
    @dataclass(frozen=True)
    class Request(CompoundScalar, abstract=True):
        symbol: str

    @dataclass(frozen=True)
    class Add(Request):
        quantity: int

    @dataclass(frozen=True)
    class Remove(Request):
        reason: str

    @graph
    def app(
        lhs: TSD[str, TS[Request]], rhs: TSD[str, TS[Remove]]
    ) -> TSD[str, TS[Request]]:
        return merge(lhs, rhs, disjoint=True)

    add = Add(symbol="ADD", quantity=2)
    remove = Remove(symbol="REMOVE", reason="expired")
    amended = Remove(symbol="REMOVE", reason="cancelled")
    assert eval_node(
        app,
        [{"add": add}, None, None],
        [{"remove": remove}, {"remove": amended}, {"remove": REMOVE}],
    ) == [
        {"add": add, "remove": remove},
        {"remove": amended},
        {"remove": REMOVE},
    ]
