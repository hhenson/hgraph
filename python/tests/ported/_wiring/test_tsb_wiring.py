# Ported from release/0.5:hgraph_unit_tests/_wiring/test_tsb_wiring.py
import pytest
from datetime import date

from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, TypeVar

import pytest
from frozendict import frozendict

from hgraph import (
    TSB,
    TSB_OUT,
    TSL,
    TSS,
    TimeSeriesSchema,
    TS,
    combine,
    compute_node,
    generator,
    graph,
    IncorrectTypeBinding,
    ParseError,
    TIME_SERIES_TYPE,
    SCALAR,
    SCALAR_1,
    AUTO_RESOLVE,
    CompoundScalar,
    SIGNAL,
    TS_SCHEMA,
    REF,
    dereference,
    nothing,
    TimeSeriesReference,
)
# ported: hgraph.arrow (combinator library) is out of scope - imported
# lazily by the single test that uses it, which skips on the gap.

from hgraph.test import eval_node

import pytest
pytestmark = pytest.mark.smoke


class MyTsb(TimeSeriesSchema):
    p1: TS[int]
    p2: TS[str]


@compute_node(valid=[])
def create_my_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
    out = {}
    if ts1.modified:
        out["p1"] = ts1.value
    if ts2.modified:
        out["p2"] = ts2.value
    return out


@compute_node
def split_my_tsb(tsb: TSB[MyTsb]) -> TS[int]:
    return tsb.as_schema.p1.delta_value


@pytest.mark.parametrize(
    "ts1,ts2,expected",
    [
        [[1, 2], ["a", "b"], [dict(p1=1, p2="a"), dict(p1=2, p2="b")]],
        [[None, 2], ["a", None], [dict(p2="a"), dict(p1=2)]],
        [[1, None], [None, "b"], [dict(p1=1), dict(p2="b")]],
    ],
)
def test_tsb_output(ts1, ts2, expected):

    assert eval_node(create_my_tsb, ts1, ts2) == expected


def test_eval_node_replays_a_tsb_with_a_set_child():
    class Calendar(TimeSeriesSchema):
        holidays: TSS[date]
        start_of_week: TS[int]

    @graph
    def start(calendar: TSB[Calendar]) -> TS[int]:
        return calendar.start_of_week

    assert eval_node(start, [{
        "holidays": frozenset({date(2025, 1, 1)}),
        "start_of_week": 2,
    }]) == [2]


def test_tsb_splitting():

    @graph
    def split_tester(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = TSB[MyTsb].from_ts(p1=ts1, p2=ts2)
        return tsb.as_schema.p1

    assert eval_node(split_tester, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_copy_with_replaces_ports_and_constants():
    @graph
    def replace_port(ts1: TS[int], ts2: TS[str], replacement: TS[str]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1, p2=ts2).copy_with(p2=replacement)

    @graph
    def replace_constant(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1, p2=ts2).copy_with(p2="fixed")

    assert eval_node(replace_port, [1], ["old"], ["new"]) == [frozendict(p1=1, p2="new")]
    assert eval_node(replace_constant, [1], ["old"]) == [frozendict(p1=1, p2="fixed")]


def test_tsb_copy_with_rejects_unknown_fields():
    @graph
    def invalid(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1, p2=ts2).copy_with(missing=1)

    with pytest.raises(TypeError, match="unknown TSB field"):
        eval_node(invalid, [1], ["value"])


def test_tsb_from_ts_with_nothing_defaults():

    @graph
    def nothing_tester(ts1: TS[int]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1)

    assert eval_node(nothing_tester, [1, 2]) == [frozendict(p1=1), frozendict(p1=2)]


def test_tsb_splitting_peered():

    @graph
    def split_tester(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = create_my_tsb(ts1, ts2)
        return tsb.as_schema.p1

    assert eval_node(split_tester, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_input_not_peered():

    @graph
    def tsb_in_non_peered(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = TSB[MyTsb].from_ts(p1=ts1, p2=ts2)
        return split_my_tsb(tsb)

    assert eval_node(tsb_in_non_peered, [1, 2], ["a", "b"]) == [1, 2]


def test_tsb_input_peered():
    @graph
    def tsb_in_non_peered(ts1: TS[int], ts2: TS[str]) -> TS[int]:
        tsb: TSB[MyTsb] = create_my_tsb(ts1, ts2)
        return split_my_tsb(tsb)

    assert eval_node(tsb_in_non_peered, [1, 2], ["a", "b"]) == [1, 2]


def test_ts_schema_error():

    try:

        @graph
        def tsb_bad_return_type() -> MyTsb: ...

        assert False, "Should have raised an exception"
    except ParseError:
        ...


def test_tsb_from_scalar():
    @dataclass
    class MyScalar(CompoundScalar):
        p1: int
        p2: str

    @graph
    def g(ts: TSB[MyScalar]) -> TS[int]:
        return ts.p1

    assert eval_node(g, MyScalar(1, "a")) == [1]


def test_generic_tsb():
    from frozendict import frozendict as fd

    class GenericTSB(TimeSeriesSchema, Generic[SCALAR]):
        p1: TS[SCALAR]

    @graph
    def tsb_multi_type(
        ts: TSB[GenericTSB[SCALAR]], v: TS[SCALAR_1], _v_tp: type[SCALAR_1] = AUTO_RESOLVE
    ) -> TSB[GenericTSB[SCALAR_1]]:
        return TSB[GenericTSB[_v_tp]].from_ts(p1=v)

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TSB[GenericTSB[str]]:
        return tsb_multi_type(TSB[GenericTSB[int]].from_ts(p1=ts1), ts2)

    assert eval_node(g, [1, 2], ["a", "b"]) == [fd({"p1": "a"}), fd({"p1": "b"})]


def test_generic_tsb_accepts_structured_scalar_specialization():
    class LeftOrder(TimeSeriesSchema):
        value: TS[int]

    class RightOrder(TimeSeriesSchema):
        value: TS[str]

    Order = TypeVar("Order", LeftOrder, RightOrder)

    class OrderState(TimeSeriesSchema, Generic[Order]):
        requested: TSB[Order]
        confirmed: TSB[Order]

    @dataclass(frozen=True)
    class ConcreteOrderType:
        value: int

    # Python's runtime GenericAlias does not enforce TypeVar constraints, and
    # released hgraph therefore permits downstream declarations like this at
    # import time. TSB already lifts dataclass scalar schemas; generic schema
    # specialization must use the same conversion path.
    concrete = TSB[OrderState[ConcreteOrderType]]
    assert concrete.handle.is_tsb


def test_tsb_order_preservation():
    @compute_node
    def n1(a: TS[str], b: TS[str]) -> TS[str]:
        return a.value + b.value

    @compute_node
    def n2(b: TS[str], a: TS[str]) -> TS[str]:
        return a.value + b.value

    @graph
    def g(a: TS[str], b: TS[str]) -> TS[str]:
        return n1(a, b) + n2(a, b)

    assert eval_node(g, "a", "b") == ["abba"]


def test_free_tsb_signal():
    @compute_node
    def s(ts: SIGNAL) -> TS[bool]:
        return True

    @graph
    def tsb_non_peered(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1, p2=ts2)

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[bool]:
        return s(tsb_non_peered(ts1, ts2))

    assert eval_node(g, [None, 1, None], [None, None, "b"]) == [None, True, True]


def test_free_tsb_ref_signal():
    @compute_node
    def s(ts: SIGNAL) -> TS[bool]:
        return True

    @compute_node(valid=())
    def tsb_peered(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return dict(p1=ts1.value, p2=ts2.value)

    @graph
    def tsb_non_peered(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return TSB[MyTsb].from_ts(p1=ts1, p2=ts2)

    @graph
    def g(c: TS[bool], ts1: TS[int], ts2: TS[str]) -> TS[bool]:
        from hgraph import switch_

        return s(switch_(c, {True: tsb_non_peered, False: tsb_peered}, ts1, ts2))

    assert eval_node(g, [None, False, True, False], [None, 1, None], [None, None, "b"]) == [
        None,
        True,
        True,
        True,
    ]


def test_tsb_inline_schema():
    @graph
    def g(tsb: TSB["lhs" : TS[int], "rhs" : TS[int]]) -> TS[int]:
        return tsb.lhs + tsb.rhs

    assert eval_node(g, [{"lhs": 1, "rhs": 2}]) == [3]


def test_tsb_kwargs():

    @graph
    def g(tsb: TSB[MyTsb]) -> TS[int]:
        values = dict(**tsb)
        return values["p1"]

    assert eval_node(g, [{"p1": 1, "p2": "a"}, {"p1": 2}]) == [1, 2]


def test_tsb_as_dict():

    @graph
    def g(tsb: TSB[MyTsb]) -> TS[str]:
        return tsb.as_dict()["p2"]

    assert eval_node(g, [{"p1": 1, "p2": "a"}, {"p2": "b"}]) == ["a", "b"]


def test_tsb_integer_index():

    @graph
    def g(tsb: TSB[MyTsb]) -> TS[int]:
        return tsb[0]

    assert eval_node(g, [{"p1": 1, "p2": "a"}, {"p1": 2}]) == [1, 2]


def test_tsb_ref_index():

    @graph
    def g(tsb: REF[TSB[TS_SCHEMA]]) -> TS[int]:
        first, second = tsb
        return first

    from hgraph.arrow import eval_, if_then, c, assert_

    eval_(True, "") | if_then(c(1) / c("a")) >> g >> assert_(1)


def test_dereference_materializes_tsb_field_references():
    @compute_node
    def as_reference(tsb: REF[TSB[MyTsb]]) -> REF[TSB[MyTsb]]:
        return tsb.value

    @graph
    def g(tsb: TSB[MyTsb]) -> TSB[MyTsb]:
        fields = dereference(as_reference(tsb))
        assert fields.p1.output_type == REF[TS[int]]
        assert fields.p2.output_type == REF[TS[str]]
        return TSB[MyTsb].from_ts(p1=fields.p1, p2=fields.p2)

    assert eval_node(g, [{"p1": 1}, {"p2": "a"}, {"p1": 2}]) == [
        {"p1": 1},
        {"p2": "a"},
        {"p1": 2},
    ]


def test_dereference_rejects_incompatible_non_peered_tsb_child_references():
    class WrongTsb(TimeSeriesSchema):
        p1: TS[str]
        p2: TS[str]

    @compute_node
    def misdeclare_reference(
        tsb: REF[TSB[WrongTsb]],
    ) -> REF[TSB[MyTsb]]:
        return tsb.value

    @graph
    def g(p1: TS[str], p2: TS[str]) -> TS[int]:
        tsb = TSB[WrongTsb].from_ts(p1=p1, p2=p2)
        return dereference(misdeclare_reference(tsb)).p1

    with pytest.raises(RuntimeError, match="reference targets schema"):
        eval_node(g, ["wrong"], ["value"])


def test_tsb_output_access():
    @compute_node
    def f(tsb: TSB[MyTsb], _output: TSB_OUT[MyTsb] = None) -> TSB[MyTsb]:
        if tsb.p1.value != _output.p1.value:
            return tsb.delta_value

    @graph
    def g(ts1: TS[int], ts2: TS[str]) -> TS[str]:
        tsb: TSB[MyTsb] = TSB[MyTsb].from_ts(p1=ts1, p2=ts2)
        return f(tsb).p2

    assert eval_node(g, [1, 1, 2], ["a", "b", "c"]) == ["a", None, "c"]
    

def test_tsb_ref_flipping():
    @generator
    def null_ref(tpe: type[TSB[MyTsb]]) -> REF[TSB[MyTsb]]:
        yield timedelta(), TimeSeriesReference.make()
    
    @graph
    def g(tsb1: TSB[MyTsb], tsb2: TSB[MyTsb], i: TS[int]) -> TSB[MyTsb]:
        return combine[TSL](tsb1, tsb2, null_ref(TSB[MyTsb]))[i]
    
    assert eval_node(g, [{"p1": 1}, {"p2": "a"}], [{"p1": 2}, {"p2": "b"}], [0, 2, 1, 2]) == [
        {"p1": 1},
        None,
        {"p1": 2, "p2": "b"},
        None,
    ]
