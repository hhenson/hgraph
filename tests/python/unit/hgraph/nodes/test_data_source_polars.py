import polars as pl

from hgraph import MIN_ST, MIN_TD, graph, TSB, TS, ts_schema, TSD, TS_SCHEMA, GlobalState
from hgraph.test import eval_node
from hgraph.nodes import from_polars, debug_print, to_polars, get_polars_df


def test_from_polars_as_data_frame():

    @graph
    def polars_graph() -> TSB[ts_schema(a=TS[int], b=TS[float])]:
        df = pl.DataFrame(
            {"time": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2], "a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        out = from_polars(df, "time")
        # debug_print("out", out)
        return out

    assert eval_node(polars_graph) == [{'a': 1, 'b': 4.0}, {'a': 2, 'b': 5.0}, {'a': 3, 'b': 6.0}]


def test_from_polars_as_tsd():

    @graph
    def polars_graph() -> TSD[str, TS[float]]:
        df = pl.DataFrame(
            {"time": [MIN_ST, MIN_ST + MIN_TD, MIN_ST + MIN_TD * 2], "a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]})
        out = from_polars(df, "time", as_bundle=False)
        # debug_print("out", out)
        return out

    assert eval_node(polars_graph) == [{'a': 1.0, 'b': 4.0}, {'a': 2.0, 'b': 5.0}, {'a': 3.0, 'b': 6.0}]


def test_to_polars():

    @graph
    def polars_graph(ts: TSB[TS_SCHEMA]):
        to_polars(ts)

    with GlobalState():
        eval_node(polars_graph,
                         [{'a': 1, 'b': 4.0}, {'a': 2, 'b': 5.0}, {'a': 3, 'b': 6.0}],
                         resolution_dict={"ts": TSB[ts_schema(a=TS[int], b=TS[float])]})

        df = get_polars_df()
        assert df.shape == (3, 3)
        assert list(df["a"]) == [1, 2, 3]
        assert list(df["b"]) == [4.0, 5.0, 6.0]
