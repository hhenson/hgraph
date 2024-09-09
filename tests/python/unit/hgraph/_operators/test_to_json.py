from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Mapping

import pytest
from frozendict import frozendict as fd

from hgraph import TIME_SERIES_TYPE, TS, to_json, CompoundScalar, from_json
from hgraph.test import eval_node


class ExpEnum(Enum):
    E1 = 1
    E2 = 2


@dataclass
class MyCS(CompoundScalar):
    p1: str
    p2: date


@pytest.mark.parametrize(
    ["tp", "value", "expected"],
    [
        [TS[int], 1, "1"],
        [TS[float], 1.0, "1.0"],
        [TS[date], date(2024, 6, 13), '"2024-06-13"'],
        [TS[datetime], datetime(2024, 6, 13, 10, 15, 30, 42), '"2024-06-13 10:15:30.000042"'],
        [TS[time], time(10, 15, 30, 42), '"10:15:30.000042"'],
        [TS[timedelta], timedelta(10, 15, microseconds=42), '"10:0:0:15.000042"'],
        [TS[ExpEnum], ExpEnum.E1, '"E1"'],
        [TS[MyCS], MyCS(p1="a", p2=date(2024, 6, 13)), '{ "p1": "a", "p2": "2024-06-13" }'],
        [TS[Mapping[str, int]], fd(p1=1, p2=2), '{ "p1": 1, "p2": 2 }'],
        [TS[tuple[str, ...]], ("1", "2"), '[ "1", "2" ]'],
    ],
)
def test_to_json(tp: TIME_SERIES_TYPE, value: Any, expected: str):
    assert eval_node(to_json[tp], [value]) == [expected]
    assert eval_node(from_json[tp], [expected]) == [value]
