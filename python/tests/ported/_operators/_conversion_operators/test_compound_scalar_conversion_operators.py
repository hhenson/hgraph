from dataclasses import dataclass, field

from hgraph import graph, TS, combine, CompoundScalar, TSB, convert, if_
from hgraph.test import eval_node




def test_combine_cs():

    @dataclass
    class AB(CompoundScalar):
        a: int
        b: str = "b"

    @graph
    def g(a: TS[int], b: TS[str]) -> TS[AB]:
        return combine[TS[AB]](a=a, b=b)

    assert eval_node(g, [None, 1], "a") == [None, AB(a=1, b="a")]

    @graph
    def h(a: TS[int], b: TS[str]) -> TS[AB]:
        return combine[TS[AB]](a=a, b=b, __strict__=False)

    assert eval_node(h, [None, 1], "a") == [AB(a=None, b="a"), AB(a=1, b="a")]

    @graph
    def u(a: TS[int]) -> TS[AB]:
        return combine[TS[AB]](a=a, b="a")

    assert eval_node(u, [None, 1]) == [None, AB(a=1, b="a")]

    @graph
    def v(a: TS[int]) -> TS[AB]:
        return combine[TS[AB]](a=a)

    assert eval_node(v, [None, 1]) == [None, AB(a=1, b="b")]


def test_combine_cs_boxes_concrete_value_for_object_field():
    @dataclass(frozen=True)
    class Box(CompoundScalar):
        value: object

    @graph
    def build(value: TS[float]) -> TS[Box]:
        return combine[TS[Box]](value=value)

    assert eval_node(build, [1.5]) == [Box(value=1.5)]


def test_convert_cs():
    @dataclass
    class AB(CompoundScalar):
        a: int
        b: str

    @graph
    def g(x: TSB[AB]) -> TS[AB]:
        return convert[TS[AB]](x)

    assert eval_node(g, [dict(a=None, b="a"), dict(a=1)]) == [None, AB(a=1, b="a")]

    @graph
    def h(x: TSB[AB]) -> TS[AB]:
        return convert[TS[CompoundScalar]](x)

    assert eval_node(h, [dict(a=None, b="a"), dict(a=1)]) == [None, AB(a=1, b="a")]

    @graph
    def g1(x: TSB[AB]) -> TS[AB]:
        return convert[TS[AB]](x, __strict__=False)

    assert eval_node(g1, [dict(b="a")]) == [AB(a=None, b="a")]

    @graph
    def h1(x: TSB[AB]) -> TS[AB]:
        return convert[TS[CompoundScalar]](x, __strict__=False)

    assert eval_node(h1, [dict(b="a")]) == [AB(a=None, b="a")]

    @graph
    def u(x: TS[AB]) -> TSB[AB]:
        return convert[TSB](x)
    
    assert eval_node(u, [None, AB(a=1, b="a")]) == [None, dict(a=1, b="a")]

    @graph
    def as_scalar(x: TSB[AB]) -> TS[AB]:
        return x.as_scalar_ts()

    assert eval_node(as_scalar, [dict(a=None, b="a"), dict(a=1)]) == [None, AB(a=1, b="a")]

    @graph
    def ref_as_scalar(x: TSB[AB]) -> TS[AB]:
        return if_(True, x).true.as_scalar_ts()

    assert eval_node(ref_as_scalar, [dict(a=1, b="a")]) == [AB(a=1, b="a")]


def test_convert_tsb_to_compound_scalar_preserves_nested_derived_value():
    @dataclass(frozen=True)
    class Explain(CompoundScalar, abstract=True):
        symbol: str

    @dataclass(frozen=True)
    class ExplainLeaf(Explain):
        detail: str

    @dataclass(frozen=True)
    class Price(CompoundScalar):
        value: float
        explain: Explain

    expected = Price(
        value=42.0,
        explain=ExplainLeaf(symbol="ABC", detail="derived detail"),
    )

    @graph
    def strict(explain: TS[ExplainLeaf]) -> TS[Price]:
        value = combine[TSB[Price]](value=42.0, explain=explain)
        return convert[TS[Price]](value)

    @graph
    def lenient(explain: TS[ExplainLeaf]) -> TS[Price]:
        value = combine[TSB[Price]](value=42.0, explain=explain)
        return convert[TS[Price]](value, __strict__=False)

    assert eval_node(strict, [expected.explain]) == [expected]
    assert eval_node(lenient, [expected.explain]) == [expected]
    assert type(eval_node(strict, [expected.explain])[0].explain) is ExplainLeaf


def test_convert_tsb_to_compound_scalar_uses_constructor_defaults():
    @dataclass(frozen=True)
    class WithDefault:
        value: int
        label: str = "default"
        computed: str = field(default="computed", init=False)

    @graph
    def convert_partial(value: TSB[WithDefault]) -> TS[WithDefault]:
        return convert[TS[WithDefault]](value)

    assert eval_node(convert_partial, [dict(value=1)]) == [WithDefault(value=1)]
