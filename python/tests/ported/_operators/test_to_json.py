from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import Any, Mapping, Set

import pytest
import json
from frozendict import frozendict as fd

from hgraph import (
    TIME_SERIES_TYPE,
    TS,
    to_json,
    CompoundScalar,
    from_json,
    Size,
    TSL,
    TSB,
    TSS,
    TSD,
    Removed,
    REMOVE,
    to_json_builder,
    from_json_builder,
    register_json_datetime_format,
)
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


@dataclass
class MyNullableMappingCS(CompoundScalar):
    data: Mapping[str, str] = None


@pytest.mark.parametrize(
    ["tp", "value", "expected"],
    [
        [TS[int], 1, "1"],
        [TS[float], 1.0, "1.0"],
        [TS[date], date(2024, 6, 13), '"2024-06-13"'],
        # deviation: RFC 0002 version-2 writers use canonical UTC/signed-us
        # text. The schema-directed reader retains upstream version-1 support.
        [TS[datetime], datetime(2024, 6, 13, 10, 15, 30, 42), '"2024-06-13T10:15:30.000042Z"'],
        [TS[time], time(10, 15, 30, 42), '"10:15:30.000042"'],
        [TS[timedelta], timedelta(10, 15, microseconds=42), '"864015000042us"'],
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


def test_from_json_accepts_legacy_temporal_version_1_text():
    assert eval_node(
        from_json[TS[datetime]],
        ['"2024-06-13 10:15:30.000042"'],
    ) == [datetime(2024, 6, 13, 10, 15, 30, 42)]
    assert eval_node(
        from_json[TS[timedelta]],
        ['"10:0:0:15.000042"', '"-1:23:59:59.999999"'],
    ) == [
        timedelta(days=10, seconds=15, microseconds=42),
        timedelta(microseconds=-1),
    ]


@pytest.mark.parametrize(
    ["text", "expected"],
    [
        ['"2024-06-13T10:15:30.000042"', datetime(2024, 6, 13, 10, 15, 30, 42)],
        ['"2024-06-13T10:15:30"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"2024-06-13T10:15"', datetime(2024, 6, 13, 10, 15)],
        ['"2024-06-13"', datetime(2024, 6, 13)],
        ['"2024-06-13T10:15:30.5"', datetime(2024, 6, 13, 10, 15, 30, 500000)],
        ['"20240613T101530"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"20240613101530"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"20240613101530123456"', datetime(2024, 6, 13, 10, 15, 30, 123456)],
        ['"2024-06-13 10:15:30.000042"', datetime(2024, 6, 13, 10, 15, 30, 42)],
        ['"2024-06-13T10:15:30Z"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"2024-06-13T11:15:30+01:00"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"2024-06-13T05:15:30-05:00"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"2024/06/13 10:15:30"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"13-Jun-2024 10:15:30"', datetime(2024, 6, 13, 10, 15, 30)],
        ['"13 Jun 2024"', datetime(2024, 6, 13)],
    ],
)
def test_from_json_accepts_many_datetime_formats(text: str, expected: datetime):
    assert eval_node(from_json[TS[datetime]], [text]) == [expected]


@pytest.mark.parametrize(
    ["text", "expected"],
    [
        ['"10:15:30.000042"', time(10, 15, 30, 42)],
        ['"10:15:30"', time(10, 15, 30)],
        ['"10:15"', time(10, 15)],
        ['"101530"', time(10, 15, 30)],
        ['"101530123456"', time(10, 15, 30, 123456)],
    ],
)
def test_from_json_accepts_many_time_formats(text: str, expected: time):
    assert eval_node(from_json[TS[time]], [text]) == [expected]


@pytest.mark.parametrize(
    ["text", "expected"],
    [
        ['"2024-06-13"', date(2024, 6, 13)],
        ['"20240613"', date(2024, 6, 13)],
        ['"2024/06/13"', date(2024, 6, 13)],
        ['"13-Jun-2024"', date(2024, 6, 13)],
        ['"2024-06-13T10:15:30"', date(2024, 6, 13)],
    ],
)
def test_from_json_accepts_many_date_formats(text: str, expected: date):
    assert eval_node(from_json[TS[date]], [text]) == [expected]


def test_from_json_rejects_an_unparseable_datetime():
    with pytest.raises(Exception, match="not a datetime"):
        eval_node(from_json[TS[datetime]], ['"not a datetime"'])


def test_register_json_datetime_format_accepts_a_producers_own_format():
    register_json_datetime_format("%d/%m/%Y %H:%M:%S")
    assert eval_node(
        from_json[TS[datetime]], ['"13/06/2024 10:15:30"'],
    ) == [datetime(2024, 6, 13, 10, 15, 30)]


def test_registered_json_datetime_format_translates_fraction_directives():
    register_json_datetime_format("%Y-%m-%d %H:%M:%S,%f")
    assert eval_node(
        from_json[TS[datetime]],
        ['"2024-06-13 10:15:30,123456"'],
    ) == [datetime(2024, 6, 13, 10, 15, 30, 123456)]


def test_register_json_time_format_accepts_a_producers_own_format():
    register_json_datetime_format("%I.%M.%S %p", time_only=True)
    assert eval_node(
        from_json[TS[time]],
        ['"10.15.30 PM"', '"12.00.00 AM"', '"12.00.00 PM"'],
    ) == [
        time(22, 15, 30),
        time(0, 0, 0),
        time(12, 0, 0),
    ]


def test_registered_json_time_format_translates_fraction_directives():
    register_json_datetime_format("%H:%M:%S,%f", time_only=True)
    assert eval_node(
        from_json[TS[time]], ['"10:15:30,000042"'],
    ) == [time(10, 15, 30, 42)]


def test_to_json_omits_fractional_seconds_when_zero():
    # RFC 0002 keeps the canonical UTC suffix for an Instant while matching
    # Python's omission of a zero fractional component.
    assert eval_node(
        to_json[TS[datetime]], [datetime(2024, 6, 13, 10, 15, 30)],
    ) == ['"2024-06-13T10:15:30Z"']
    assert eval_node(to_json[TS[time]], [time(10, 15, 30)]) == [
        '"10:15:30"'
    ]


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


def test_from_json_accepts_the_0_5_call_shapes():
    # release/0.5 declared ``from_json_generic(ts, _tp=AUTO_RESOLVE,
    # delta=False)`` and ``to_json_generic(ts, _tp=AUTO_RESOLVE, delta=False)``,
    # so ported code may pass either explicitly. Both spellings must keep
    # wiring; only ``to_json``'s ``delta`` alters output.
    assert eval_node(from_json[TS[int]], ['1', '2'], delta=True) == [1, 2]
    assert eval_node(from_json[TS[int]], ['1', '2'], delta=False) == [1, 2]
    assert eval_node(from_json[TS[int]], ['1', '2'], _tp=TS[int]) == [1, 2]
    assert eval_node(to_json[TS[int]], [1, 2], _tp=TS[int]) == ['1', '2']


def test_json_operators_accept_a_positional_type_carrier():
    # 0.5 declared ``_tp`` positional-or-keyword, so ported code may pass the
    # type expression positionally - and then ``delta`` as the third argument.
    # Without absorbing the carrier the type expression lands on the native
    # ``delta`` scalar and the call fails to wire.
    assert eval_node(from_json[TS[int]], ['1', '2'], TS[int]) == [1, 2]
    assert eval_node(to_json[TS[int]], [1, 2], TS[int]) == ['1', '2']
    assert eval_node(to_json[TS[int]], [1, 2], TS[int], True) == ['1', '2']


def test_from_json_applies_a_bare_set_array_as_a_delta():
    # release/0.5 parity: a bare array ADDS, leaving absent members alone.
    # Treating it as a whole-set replace removed 1 and 2 on the second tick.
    assert eval_node(from_json[TSS[int]], ['[1, 2]', '[3]']) == [{1, 2}, {3}]
    # Removal stays available through the explicit delta form.
    assert eval_node(
        from_json[TSS[int]], ['[1, 2]', '{"removed": [2]}']
    ) == [{1, 2}, {Removed(2)}]


def test_from_json_treats_a_null_tsl_element_as_no_tick():
    # release/0.5 parity: null means "this element has no value this tick".
    # Parsing the array as one value rejected null against a typed element and
    # failed the whole call.
    assert eval_node(
        from_json[TSL[TS[int], Size[2]]], ['[1, 2]', '[null, 9]']
    ) == [{0: 1, 1: 2}, {1: 9}]


def test_to_json_builder_serializes_none_mapping_value_as_null():
    expected = MyNullableMappingCS(data={"Key": None})

    payload = to_json_builder(type(expected))(expected)
    payload_dict = json.loads(payload)

    assert payload_dict == {"data": {"Key": None}}
    assert from_json_builder(type(expected))(payload_dict) == expected
