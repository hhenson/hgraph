from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from frozendict import frozendict

from hgraph import CompoundScalar, Frame, Series, TS, TSD, compound_scalar, filter_, graph
from hgraph.adaptors.data_frame import (
    concat,
    filter_cs,
    filter_exp,
    filter_exp_seq,
    filter_frame,
    join,
    sorted_,
    ungroup,
    with_columns,
)
from hgraph.adaptors.data_frame._data_frame_operators import filter_exp_ts
from hgraph.test import eval_node


@dataclass(frozen=True)
class AB(CompoundScalar):
    a: int
    b: int


def test_join():
    left = pa.table({"a": [1, 2], "b": [10, 20]})
    right = pa.table({"a": [2, 3], "b": [200, 300]})
    out = compound_scalar(a=int, b=int, b_right=int)

    @graph
    def app(lhs: TS[Frame[AB]], rhs: TS[Frame[AB]]) -> TS[Frame[out]]:
        return join(lhs, rhs, on="a", how="left")

    assert eval_node(app, [left], [right])[0].equals(
        pa.table({"a": [2, 1], "b": [20, 10], "b_right": [200, None]})
    )


def test_join_semi_and_anti():
    left = pa.table({"a": [1, 2], "b": [10, 20]})
    right = pa.table({"a": [2, 3], "b": [200, 300]})

    @graph
    def semi(lhs: TS[Frame[AB]], rhs: TS[Frame[AB]]) -> TS[Frame[AB]]:
        return join(lhs, rhs, on="a", how="semi")

    @graph
    def anti(lhs: TS[Frame[AB]], rhs: TS[Frame[AB]]) -> TS[Frame[AB]]:
        return join(lhs, rhs, on="a", how="anti")

    assert eval_node(semi, [left], [right])[0].equals(left.slice(1, 1))
    assert eval_node(anti, [left], [right])[0].equals(left.slice(0, 1))


def test_filter_variants():
    table = pa.table({"a": [1, 2, 3], "b": [10, 20, 30]})

    predicate = compound_scalar(a=int, b=int)
    sparse_condition = predicate(a=2)
    assert sparse_condition.b is None
    assert eval_node(
        filter_cs,
        [table],
        sparse_condition,
        resolution_dict={"ts": TS[Frame[predicate]]},
    )[0].equals(
        table.slice(1, 1)
    )
    assert eval_node(filter_exp, [table], pc.field("a") > 1, resolution_dict={"ts": TS[Frame[AB]]})[0].equals(
        table.slice(1)
    )
    assert eval_node(
        filter_exp_ts,
        [table],
        [pc.field("b") < 30],
        resolution_dict={"ts": TS[Frame[AB]]},
    )[0].equals(table.slice(0, 2))
    assert eval_node(
        filter_exp_seq,
        [table],
        (pc.field("a") > 1, pc.field("b") < 30),
        resolution_dict={"ts": TS[Frame[AB]]},
    )[0].equals(table.slice(1, 1))

    @graph
    def filter_kwargs(ts: TS[Frame[AB]], a: TS[int]) -> TS[Frame[AB]]:
        return filter_frame(ts, a=a)

    assert eval_node(filter_kwargs, [table], [3])[0].equals(table.slice(2, 1))


def test_arrow_expression_operator_and_filter_overload():
    table = pa.table({"a": [1, 2, 3], "b": [10, 20, 30]})

    @graph
    def app(ts: TS[Frame[AB]], expression: TS[pc.Expression], threshold: TS[int]) -> TS[Frame[AB]]:
        return filter_(expression > threshold, ts)

    result = eval_node(app, [table], [pc.field("a")], [1])
    assert result[0].equals(table.slice(1))


def test_sorted_and_concat():
    first = pa.table({"a": [2, 1], "b": [20, 10]})
    second = pa.table({"a": [3], "b": [30]})

    assert eval_node(sorted_, [first], by="a", resolution_dict={"ts": TS[Frame[AB]]})[0].equals(
        pa.table({"a": [1, 2], "b": [10, 20]})
    )
    assert eval_node(
        concat,
        [first],
        [second],
        resolution_dict={"ts1": TS[Frame[AB]], "ts2": TS[Frame[AB]]},
    )[0].equals(pa.concat_tables([first, second]))


def test_ungroup_default_and_with_keys():
    one = pa.table({"b": [10, 20]})
    two = pa.table({"b": [30]})
    row = compound_scalar(b=int)
    keyed_row = compound_scalar(b=int, parent=str, child=str)

    @graph
    def plain(ts: TSD[str, TS[Frame[row]]]) -> TS[Frame[row]]:
        return ungroup(ts)

    @graph
    def keyed(ts: TSD[tuple[str, str], TS[Frame[row]]]) -> TS[Frame[keyed_row]]:
        return ungroup(ts, ("parent", "child"), keyed_row)

    @graph
    def keyed_inferred(ts: TSD[tuple[str, str], TS[Frame[row]]]) -> TS[Frame[keyed_row]]:
        return ungroup(ts, ("parent", "child"))

    assert eval_node(plain, [frozendict(one=one, two=two)])[0].equals(pa.concat_tables([one, two]))
    assert eval_node(keyed, [frozendict({("p", "x"): one, ("q", "y"): two})])[0].equals(
        pa.table({"b": [10, 20, 30], "parent": ["p", "p", "q"], "child": ["x", "x", "y"]})
    )
    assert eval_node(keyed_inferred, [frozendict({("p", "x"): one, ("q", "y"): two})])[0].equals(
        pa.table({"b": [10, 20, 30], "parent": ["p", "p", "q"], "child": ["x", "x", "y"]})
    )


def test_ungroup_compound_scalar_items():
    @graph
    def app(ts: TSD[str, TS[AB]]) -> TS[Frame[AB]]:
        return ungroup(ts)

    result = eval_node(app, [frozendict(x=AB(1, 10), y=AB(2, 20))])[0]
    assert result.equals(pa.table({"a": [1, 2], "b": [10, 20]}))


def test_with_columns_replace_and_project():
    table = pa.table({"a": [1, 2], "b": [10, 20]})
    projected = compound_scalar(a=int, c=int)

    @graph
    def replace(ts: TS[Frame[AB]], b: TS[int]) -> TS[Frame[AB]]:
        return with_columns(ts, b=b)

    @graph
    def project(ts: TS[Frame[AB]], c: TS[int]) -> TS[Frame[projected]]:
        return with_columns[projected](ts, c=c)

    assert eval_node(replace, [table], [99])[0].equals(pa.table({"a": [1, 2], "b": [99, 99]}))
    assert eval_node(project, [table], [7])[0].equals(pa.table({"a": [1, 2], "c": [7, 7]}))


def test_with_columns_accepts_a_typed_series_column():
    table = pa.table({"a": [1, 2], "b": [10, 20]})
    projected = compound_scalar(a=int, c=int)

    @graph
    def app(ts: TS[Frame[AB]], c: TS[Series[int]]) -> TS[Frame[projected]]:
        return with_columns(ts, _tp_out=projected, c=c)

    assert eval_node(app, [table], [pa.array([7, 8])])[0].equals(
        pa.table({"a": [1, 2], "c": [7, 8]})
    )
