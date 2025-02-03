from typing import Mapping

from hgraph import lift, TS, HgTSLTypeMetaData, HgTSTypeMetaData, HgAtomicType, TSD
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
