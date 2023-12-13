from hg import MIN_TD
from hg.nodes import lag_ts
from hg.test import eval_node


def test_delay_ts():
    assert eval_node(lag_ts, ts=[1, 2, 3], delay=MIN_TD) == [None, 1, 2, 3]