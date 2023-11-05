import pytest

from hg import TSB, TimeSeriesSchema, TS, compute_node
from hg.test import eval_node


@pytest.mark.parametrize("ts1,ts2,expected", [
    [[1, 2], ["a", "b"], [dict(p1=1, p2="a"), dict(p1=2, p2="b")]],
    [[None, 2], ["a", None], [dict(p2="a"), dict(p1=2)]],
])
def test_tsb(ts1, ts2, expected):

    class MyTsb(TimeSeriesSchema):
        p1: TS[int]
        p2: TS[str]

    @compute_node(valid=[])
    def create_my_tsb(ts1: TS[int], ts2: TS[str]) -> TSB[MyTsb]:
        out = {}
        if ts1.modified:
            out['p1'] = ts1.value
        if ts2.modified:
            out['p2'] = ts2.value
        return out

    assert eval_node(create_my_tsb, ts1, ts2) == expected
