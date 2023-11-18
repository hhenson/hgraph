import pytest

from hg import TIME_SERIES_TYPE, compute_node, REF, TS
from hg.test import eval_node


@compute_node
def create_ref(ts: REF[TIME_SERIES_TYPE]) -> REF[TIME_SERIES_TYPE]:
    return ts.value


@pytest.mark.xfail(reason="Not implemented")
def test_ref():
    assert eval_node(create_ref[TIME_SERIES_TYPE: TS[int]], ts=[1, 2]) == [1, 2]
