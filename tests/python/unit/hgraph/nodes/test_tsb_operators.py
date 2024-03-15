from hgraph import TimeSeriesSchema, TS, TSB, graph, compute_node, TIME_SERIES_TYPE, REF
from hgraph.test import eval_node


def test_tsb_get_item():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @compute_node
    def make_ref(r: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        return r.value

    @graph
    def g(b: TSB[ABSchema]) -> TSB[ABSchema]:
        br = make_ref(b)
        return {'a': br.a + 1, 'b': br['b']}

    assert eval_node(g, [{'a': 1, 'b': '2'}]) == [{'a': 2, 'b': '2'}]


def test_tsb_get_item_by_index():
    class ABSchema(TimeSeriesSchema):
        a: TS[int]
        b: TS[str]

    @compute_node
    def make_ref(r: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
        return r.value

    @graph
    def g(b: TSB[ABSchema]) -> TSB[ABSchema]:
        br = make_ref(b)
        return {'a': br[0] + 1, 'b': br[1]}

    assert eval_node(g, [{'a': 1, 'b': '2'}]) == [{'a': 2, 'b': '2'}]