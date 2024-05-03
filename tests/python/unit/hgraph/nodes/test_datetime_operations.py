from datetime import date, datetime, timedelta

import pytest

from hgraph import lt_, sub_, TS, add_, eq_, gt_, le_, ge_, MIN_ST, MIN_TD
from hgraph.nodes import modified_date, modified_datetime
from hgraph.test import eval_node


@pytest.mark.parametrize(
    ['op', 'd1', 'd2', 'expected'],
    [
        [sub_, date(2020, 1, 2), date(2020, 1, 1), timedelta(days=1)],
        [sub_, datetime(2020, 1, 2), datetime(2020, 1, 1), timedelta(days=1)],
        [sub_, date(2020, 1, 2), timedelta(days=1), date(2020, 1, 1)],
        [sub_, datetime(2020, 1, 2), timedelta(days=1), datetime(2020, 1, 1)],
        [add_, date(2020, 1, 2), timedelta(days=1), date(2020, 1, 3)],
        [add_, datetime(2020, 1, 2), timedelta(days=1), datetime(2020, 1, 3)],
        [eq_, date(2020, 1, 2), date(2020, 1, 1), False],
        [lt_, date(2020, 1, 2), date(2020, 1, 1), False],
        [gt_, date(2020, 1, 2), date(2020, 1, 1), True],
        [eq_, date(2020, 1, 2), date(2020, 1, 2), True],
        [le_, date(2020, 1, 2), date(2020, 1, 2), True],
        [ge_, date(2020, 1, 2), date(2020, 1, 2), True],
    ]
)
def test_date_ops(op, d1, d2, expected):
    assert eval_node(op, d1, d2) == [expected]


def test_modified_date():
    assert eval_node(modified_date, [True, None, True], resolution_dict={"ts": TS[bool]}) == [MIN_ST.date(), None,
                                                                                              MIN_ST.date()]


def test_modified_datetime():
    assert eval_node(modified_datetime, [True, None, True], resolution_dict={"ts": TS[bool]})\
           == [MIN_ST, None, MIN_ST + 2 * MIN_TD]
