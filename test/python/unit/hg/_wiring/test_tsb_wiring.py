from hg import TSB, TimeSeriesSchema, TS, compute_node
from hg.test import eval_node


def test_tsb():

    class MyTsb(TimeSeriesSchema):
        p1: TS[int]
        p2: TS[str]

    @compute_node
    def create_my_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        return dict(p1=ts1.value, p2=ts2.value)

    assert eval_node(create_my_tsb, [1, 2], ["a", "b"]) == [dict(p1=1, p2="a"), dict(p1=2, p2="b")]
