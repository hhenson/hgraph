from typing import Mapping

from hgraph import lift, TS, HgTSLTypeMetaData, HgTSTypeMetaData, HgAtomicType, TSD, graph, lower, MIN_TD, MIN_ST
from hgraph.test import eval_node

from frozendict import frozendict as fd


def test_lift():

    def f(a: int, b: str = "value") -> str:
        return f"{a} {b}"

    l = lift(f)

    assert l.signature.name == "f"
    assert l.signature.input_types == {
        "a": HgTSTypeMetaData(HgAtomicType(int)),
        "b": HgTSTypeMetaData(HgAtomicType(str)),
    }
    assert l.signature.output_type == HgTSTypeMetaData(HgAtomicType(str))

    assert eval_node(l, [1], b=["v2"]) == ["1 v2"]
    assert eval_node(l, [1, 2]) == ["1 value", "2 value"]


def test_lift_override():

    def f(a: Mapping[str, int]) -> Mapping[str, int]:
        return a

    l = lift(f, output=TSD[str, TS[int]])
    assert eval_node(l, [fd(a=1)]) == [fd({"a": 1})]


def test_lift_dedup_output():
    def f(a: Mapping[str, int]) -> Mapping[str, int]:
        return a

    l = lift(f, output=TSD[str, TS[int]], dedup_output=True)
    assert eval_node(l, [fd(a=1), fd(a=1, b=2)]) == [fd({"a": 1}), fd({"b": 2})]


def test_lower():

    @graph
    def g(l: TS[int], r: TS[int]) -> TS[int]:
        return l + r

    import polars as pl

    f = lower(g, no_as_of_support=True)
    result = f(
        l=pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD], "value": [1, 2]}),
        r=pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD * 2], "value": [3, 4]}),
        # __trace__ = True
    )
    expected = pl.DataFrame({"date": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2], "value": [4, 5, 6]})
    assert expected.equals(result)
