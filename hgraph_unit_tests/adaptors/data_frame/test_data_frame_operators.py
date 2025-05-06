import polars as pl
from frozendict import frozendict

from hgraph import Frame, TS, graph, compound_scalar, TSD
from hgraph.adaptors.data_frame import schema_from_frame, join, filter_cs, ungroup
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
