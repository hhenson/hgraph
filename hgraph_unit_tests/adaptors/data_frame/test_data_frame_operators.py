import polars as pl
from frozendict import frozendict
from polars.testing import assert_frame_equal

from hgraph import Frame, TS, graph, compound_scalar, TSD
from hgraph.adaptors.data_frame import schema_from_frame, join, filter_cs, ungroup, concat, with_columns
from hgraph.test import eval_node


def test_join():
    df_1 = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df_2 = pl.DataFrame({"a": [1, 2, 3], "c": [7, 8, 9]})

    assert all(
        r.equals(e)
        for r, e in zip(
            eval_node(
                join,
                [df_1],
                [df_2],
                on="a",
                resolution_dict=dict(
                    lhs=TS[Frame[schema_from_frame(df_1)]],
                    rhs=TS[Frame[schema_from_frame(df_2)]],
                ),
            ),
            [df_1.join(df_2, on="a")],
        )
    )


def test_filter_cs():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    tp = schema_from_frame(df)
    condition = tp(a=1)
    assert all(
        r.equals(e)
        for r, e in zip(
            eval_node(
                filter_cs,
                [df],
                condition,
                resolution_dict=dict(
                    ts=TS[Frame[tp]],
                ),
            ),
            [df.filter(**condition.to_dict())],
        )
    )


def test_ungroup():
    df_1 = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    df_2 = pl.DataFrame({"a": [1, 2, 3], "b": [7, 8, 9]})
    df_3 = pl.DataFrame({"a": [], "b": []})

    scheama = compound_scalar(a=int, b=int)

    @graph
    def g(c: TSD[str, TS[Frame[scheama]]]) -> TS[Frame[scheama]]:
        v = ungroup(c)
        return v

    result = eval_node(g, [frozendict({"a": df_1, "b": df_2})])
    assert len(result[0]) == 6

    result = eval_node(g, [frozendict({"a": df_3, "b": df_3})])
    assert result is None

    result = eval_node(g, [frozendict({"a": df_1, "b": df_3})])
    assert len(result[0]) == 3



def test_ungroup_with_tuple_keys():
    df_1 = pl.DataFrame({"value": [1, 2]})
    df_2 = pl.DataFrame({"value": [3]})
    df_3 = pl.DataFrame({"value": []})

    schema = compound_scalar(value=int)
    out_schema = compound_scalar(parent=str, child=str, value=int)

    @graph
    def g(c: TSD[tuple[str, str], TS[Frame[schema]]]) -> TS[Frame[out_schema]]:
        return ungroup(c, ("parent", "child"), out_schema)

    result = eval_node(g, [frozendict({("p1", "c1"): df_1, ("p2", "c2"): df_2, ("p3", "c3"): df_3})])

    assert len(result) == 1
    assert_frame_equal(
        result[0].sort("parent", "child", "value"),
        pl.DataFrame({
            "value": [1, 2, 3],
            "parent": ["p1", "p1", "p2"],
            "child": ["c1", "c1", "c2"],
        }).sort("parent", "child", "value"),
    )


def test_ungroup_from_items():
    item_schema = compound_scalar(name=str, value=int)

    @graph
    def g(c: TSD[str, TS[item_schema]]) -> TS[Frame[item_schema]]:
        return ungroup(c)

    result = eval_node(g, [frozendict({"a": item_schema(name="alpha", value=1), "b": item_schema(name="beta", value=2)})])

    assert len(result) == 1
    assert_frame_equal(
        result[0].sort("name"),
        pl.DataFrame({"name": ["alpha", "beta"], "value": [1, 2]}).sort("name"),
    )


def test_with_columns_replaces_existing_column():
    df = pl.DataFrame({"a": [1, 2], "b": [10, 20]})
    schema = schema_from_frame(df)

    @graph
    def g(ts: TS[Frame[schema]], b: TS[int]) -> TS[Frame[schema]]:
        return with_columns(ts, b=b)

    result = eval_node(g, [df], [99])

    assert len(result) == 1
    assert_frame_equal(result[0], pl.DataFrame({"a": [1, 2], "b": pl.Series("b", [99, 99], dtype=pl.Int32)}))


def test_with_columns_with_output_schema_can_add_and_remove_columns():
    df = pl.DataFrame({"a": [1, 2], "b": [10, 20]})
    in_schema = schema_from_frame(df)
    out_schema = compound_scalar(a=int, c=int)

    @graph
    def g(ts: TS[Frame[in_schema]], c: TS[int]) -> TS[Frame[out_schema]]:
        return with_columns[out_schema](ts, c=c)

    result = eval_node(g, [df], [7])

    assert len(result) == 1
    assert_frame_equal(result[0], pl.DataFrame({"a": [1, 2], "c": pl.Series("c", [7, 7], dtype=pl.Int32)}))


def test_concat():
    df_1 = pl.DataFrame({"a": [1], "b": [4]})
    df_2 = pl.DataFrame({"a": [2], "b": [5]})
    expected = pl.concat([df_1, df_2])

    assert all(
        r.equals(e)
        for r, e in zip(
            eval_node(
                concat,
                [df_1],
                [df_2],
                resolution_dict=dict(
                    ts1=TS[Frame[schema_from_frame(df_1)]],
                    ts2=TS[Frame[schema_from_frame(df_2)]],
                ),
            ),
            [expected],
        )
    )
