from dataclasses import dataclass
from datetime import date

try:
    import polars as pl

    from hgraph import TIME_SERIES_TYPE, TS, Frame, CompoundScalar, graph, HgDataFrameScalarTypeMetaData, \
        HgCompoundScalarType, HgTypeMetaData, Array, Series
    from hgraph.nodes import pass_through
    from hgraph.test import eval_node


    @dataclass
    class TestSchema(CompoundScalar):
        a: date
        b: float


    def test_data_frame():
        assert HgTypeMetaData.parse_type(Frame[TestSchema]) == HgDataFrameScalarTypeMetaData(
            HgCompoundScalarType.parse_type(TestSchema))


    def test_data_frame1():
        frame = pl.DataFrame({'a': [date(2021, 1, 1)], 'b': [1.0]})
        assert eval_node(pass_through[TIME_SERIES_TYPE: TS[Frame[TestSchema]]], [frame]) == [frame]


    def test_data_frame_operator_1():
        frame = pl.DataFrame({'a': [date(2021, 1, 1)], 'b': [1.0]})

        @graph
        def g(ts: TIME_SERIES_TYPE) -> TS[TestSchema]:
            return ts[0]

        assert eval_node(g[TIME_SERIES_TYPE: TS[Frame[TestSchema]]], [frame]) == [TestSchema(a=date(2021, 1, 1), b=1.0)]


    def test_data_frame_operator_2():
        frame = pl.DataFrame({'a': [date(2021, 1, 1)], 'b': [1.0]})

        @graph
        def g(ts: TIME_SERIES_TYPE) -> TS[Series[date]]:
            return ts['a']

        assert eval_node(g[TIME_SERIES_TYPE: TS[Frame[TestSchema]]], [frame])[0][0] == date(2021, 1, 1)

except ImportError:
    ...
