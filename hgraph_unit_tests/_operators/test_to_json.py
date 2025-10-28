from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Mapping, Set

import pytest
import json
from frozendict import frozendict as fd

from hgraph import TIME_SERIES_TYPE, TS, to_json, CompoundScalar, from_json, Size, TSL, TSB, TSS, TSD, Removed, REMOVE
from hgraph.test import eval_node


class ExpEnum(Enum):
    E1 = 1
    E2 = 2


@dataclass
class MyCS(CompoundScalar):
    p1: str
    p2: date


@dataclass
class MyComplexCS(CompoundScalar):
    c1: tuple[MyCS, ...]


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
        [TS[MyCS], MyCS(p1="a", p2=date(2024, 6, 13)), '{"p1": "a", "p2": "2024-06-13"}'],
        [
            TS[MyComplexCS],
            MyComplexCS(c1=(MyCS(p1="a", p2=date(2024, 6, 13)),)),
            '{"c1": [{"p1": "a", "p2": "2024-06-13"}]}',
        ],
        [TS[Mapping[int, int]], {1: 1, 2: 2}, '{"1": 1, "2": 2}'],
        [TS[Mapping[str, int]], fd(p1=1, p2=2), '{"p1": 1, "p2": 2}'],
        [TS[tuple[str, ...]], ("1", "2"), '["1", "2"]'],
        [
            TS[Set[str]],
            {
                "1",
            },
            '["1"]',
        ],  # Can't have more than one as the hash is not stable
        [TSL[TS[int], Size[2]], {0: 1, 1: 2}, "[1, 2]"],
        [TSB[MyCS], {"p1": "a", "p2": date(2024, 6, 13)}, '{"p1": "a", "p2": "2024-06-13"}'],
        [TSS[int], {1, 2}, "[1, 2]"],
        [TSD[int, TS[str]], {1: "a", 2: "b"}, '{"1": "a", "2": "b"}'],
        [
            TSD[int, TSL[TS[str], Size[2]]],
            {1: {0: "a", 1: "b"}, 2: {0: "b", 1: "c"}},
            '{"1": ["a", "b"], "2": ["b", "c"]}',
        ],
    ],
)
def test_to_json(tp: TIME_SERIES_TYPE, value: Any, expected: str):
    out = eval_node(to_json[tp], [value])
    assert [json.loads(o) for o in out] == [json.loads(expected)]
    assert eval_node(from_json[tp], [expected]) == [value]


@pytest.mark.parametrize(
    ["tp", "value", "expected"],
    [
        [TSL[TS[int], Size[2]], [{0: 1, 1: 2}], ['{"0": 1, "1": 2}']],
        [TSB[MyCS], [{"p1": "a", "p2": date(2024, 6, 13)}], ['{"p1": "a", "p2": "2024-06-13"}']],
        [TSS[int], [{1, 2}, {Removed(2)}], ['{"added": [1, 2]}', '{"removed": [2]}']],
        [TSD[int, TS[str]], [{1: "a", 2: "b"}, {1: REMOVE}], ['{"1": "a", "2": "b"}', '{"1": null}']],
        [
            TSD[int, TSL[TS[str], Size[2]]],
            [{1: {0: "a", 1: "b"}, 2: {0: "b", 1: "c"}}, {1: {0: "aa"}}, {2: REMOVE}],
            ['{"1": {"0": "a", "1": "b"}, "2": {"0": "b", "1": "c"}}', '{"1": {"0": "aa"}}', '{"2": null}'],
        ],
    ],
)
def test_to_json_delta(tp: TIME_SERIES_TYPE, value: Any, expected: str):
    out = eval_node(to_json[tp], value, delta=True)
    assert [json.loads(o) for o in out] == [json.loads(e) for e in expected]
    assert eval_node(from_json[tp], expected) == value
