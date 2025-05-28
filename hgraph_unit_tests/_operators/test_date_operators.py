from datetime import date, time, datetime, timezone

import pytz

from hgraph import explode, graph, TS
from hgraph.test import eval_node


def test_explode():
    assert eval_node(explode, [date(2024, 1, 1), date(2024, 1, 2), date(2024, 2, 2), date(2025, 2, 2)]) == [
        {0: 2024, 1: 1, 2: 1},
        {2: 2},
        {1: 2},
        {0: 2025},
    ]


def test_add_date_time():
    @graph
    def g(dt: TS[date], tm: TS[time]) -> TS[datetime]:
        return dt + tm

    assert eval_node(
        g,
        [date(2024, 1, 1)],
        [
            time(10, 0, 0),
            time(10, 0, 0, tzinfo=pytz.timezone('Africa/Johannesburg'))
        ],
    ) == [
               datetime(2024, 1, 1, 10, 0, 0),
               datetime(2024, 1, 1, 8, 0, 0),
           ]
