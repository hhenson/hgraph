import polars as pl

from hgraph import Frame, TS
from hgraph.adaptors.data_frame import schema_from_frame, join, filter_cs, filter_exp
from hgraph.test import eval_node


def test_join():
    df_1 = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    df_2 = pl.DataFrame({'a': [1, 2, 3], 'c': [7, 8, 9]})

    assert all(r.equals(e) for r, e in zip(
        eval_node(join, [df_1], [df_2], on="a",
                  resolution_dict=dict(
                      lhs=TS[Frame[schema_from_frame(df_1)]],
                      rhs=TS[Frame[schema_from_frame(df_2)]],
                  )), [df_1.join(df_2, on="a")]
    ))


def test_filter_cs():
    df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    tp = schema_from_frame(df)
    condition = tp(a=1)
    assert all(r.equals(e) for r, e in zip(
        eval_node(
            filter_cs,
            [df], condition,
            resolution_dict=dict(
                ts=TS[Frame[tp]],
            )
        ),
        [df.filter(**condition.to_dict())]
    ))
