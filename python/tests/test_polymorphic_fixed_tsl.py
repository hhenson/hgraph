from dataclasses import dataclass

from hgraph import CompoundScalar, TS, TSD, graph, merge
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
