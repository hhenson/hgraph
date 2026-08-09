from dataclasses import dataclass
from datetime import timedelta

import pyarrow as pa
from frozendict import frozendict

from hgraph import (
    CompoundScalar, Frame, GlobalState, MIN_ST, MIN_TD, TS, TSB,
    TSD, graph, set_as_of,
)
from hgraph.adaptors.delta.delta_tsd_publisher import tsd_to_frame_batched
from hgraph.test import eval_node


@dataclass(kw_only=True, frozen=True)
class _Row(CompoundScalar):
    symbol: str
    value: float


@dataclass(kw_only=True, frozen=True)
class _Key(CompoundScalar):
    venue: str
    identifier: int


def _expected(rows, as_of):
    return pa.Table.from_pylist([
        {
            "__date__": timestamp.date(),
            "__timestamp__": timestamp,
            "__is_deleted__": False,
            "key": key,
            "symbol": symbol,
            "value": value,
        }
        for timestamp, key, symbol, value in rows
    ])


def _inputs():
    return [
        frozendict({"a": _Row(symbol="one", value=-100.0)}),
        frozendict({"b": _Row(symbol="two", value=200.0)}),
        frozendict({"a": _Row(symbol="one", value=-101.0)}),
        frozendict({"c": _Row(symbol="three", value=300.0)}),
    ]


def test_tsd_to_frame_batches_scalar_values():
    @graph
    def app(tsd: TSD[str, TS[_Row]]) -> TS[Frame]:
        return tsd_to_frame_batched(
            tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = MIN_ST + 10 * MIN_TD
    with GlobalState():
        set_as_of(as_of)
        out = eval_node(
            app, _inputs(),
            __end_time__=MIN_ST + timedelta(milliseconds=10), __elide__=True)

    assert out[0].equals(_expected([
        (MIN_ST, "a", "one", -100.0),
        (MIN_ST + MIN_TD, "b", "two", 200.0),
    ], as_of))
    assert out[1].equals(_expected([
        (MIN_ST + 2 * MIN_TD, "a", "one", -101.0),
        (MIN_ST + 3 * MIN_TD, "c", "three", 300.0),
    ], as_of))


def test_tsd_to_frame_batches_bundle_values():
    @graph
    def app(tsd: TSD[str, TSB[_Row]]) -> TS[Frame]:
        return tsd_to_frame_batched(
            tsd, max_rows=2, flush_period=timedelta(milliseconds=1))

    as_of = MIN_ST + 10 * MIN_TD
    with GlobalState():
        set_as_of(as_of)
        out = eval_node(
            app, _inputs()[:2],
            __end_time__=MIN_ST + timedelta(milliseconds=10), __elide__=True)

    assert out[-1].equals(_expected([
        (MIN_ST, "a", "one", -100.0),
        (MIN_ST + MIN_TD, "b", "two", 200.0),
    ], as_of))


def test_tsd_to_frame_stringifies_compound_keys():
    key = _Key(venue="X", identifier=1)

    @graph
    def app(tsd: TSD[_Key, TS[_Row]]) -> TS[Frame]:
        return tsd_to_frame_batched(
            tsd, max_rows=1, flush_period=timedelta(milliseconds=1))

    out = eval_node(
        app, [frozendict({key: _Row(symbol="one", value=1.0)})],
        __end_time__=MIN_ST + timedelta(milliseconds=2), __elide__=True)

    assert out[0]["key"].to_pylist() == ["{venue: X, identifier: 1}"]
