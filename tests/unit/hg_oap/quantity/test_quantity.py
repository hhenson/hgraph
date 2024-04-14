from hgraph import graph, TSB
from hgraph.nodes import const
from hgraph.test import eval_node

from frozendict import frozendict as fd

from hg_oap.quanity.measures import Kilogram, Gram
from hg_oap.quanity.quantity import Quantity, convert_to, UNIT_1


def test_quantity():
    @graph
    def g(ts: TSB[Quantity[Kilogram]]) -> TSB[Quantity[Gram]]:
        return convert_to(ts, const(Gram()))

    assert eval_node(g, [fd({"qty": 1.0, "unit": Kilogram()})]) == [{"qty": 1000.0, "unit": Gram()}]
