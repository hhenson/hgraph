from datetime import datetime, date

from hgraph import graph, register_service, default_path, TS
from hgraph.test import eval_node

from hg_oap.dates import WeekendCalendar
from hg_oap.dates.date_services import business_days_from_calendar, business_days


def test_business_days_from_calendar():
    @graph
    def g() -> TS[date]:
        register_service(default_path, business_days_from_calendar, calendar_tp=WeekendCalendar)
        return business_days()

    assert eval_node(
        g,
        __elide__=True,
        __start_time__=datetime(2024, 1, 4, 1),
        __end_time__=datetime(2024, 1, 8, 1)
    ) == [date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)]
