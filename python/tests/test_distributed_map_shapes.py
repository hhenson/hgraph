"""Public map_/dmap_ parity across call shapes and time-series boundaries.

Children are module-level so the same recipe is exercised in fresh interpreters.
The ordinary-map runs independently establish each expected observable trace.
"""
from pathlib import Path
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
import json
import os

import hgraph as hg
import pytest


# This opt-in process test deliberately interns extra zone names before the
# module's schemas/fixtures are imported. ZoneId storage handles must never be
# mistaken for a stable cross-process partition key.
if (os.environ.get("HGRAPH_DMAP_SHAPES_PARENT_PID")
        and os.environ["HGRAPH_DMAP_SHAPES_PARENT_PID"] != str(os.getpid())):
    _worker_interned_zones = tuple(hg.ZoneId(name) for name in (
        "Pacific/Auckland", "America/Los_Angeles", "Asia/Tokyo", "Europe/Paris"))


class Row(hg.TimeSeriesSchema):
    value: hg.TS[int]
    label: hg.TS[str]


class Nested(hg.TimeSeriesSchema):
    row: hg.TSB[Row]
    keys: hg.TSS[str]
    values: hg.TSD[str, hg.TS[int]]
    pair: hg.TSL[hg.TS[int], hg.Size[2]]


@hg.compute_node
def add_config(lhs: hg.TS[int], rhs: hg.TS[int], factor: int = 2) -> hg.TS[int]:
    return (lhs.value + rhs.value) * factor


@hg.graph
def identity(value: hg.TIME_SERIES_TYPE) -> hg.TIME_SERIES_TYPE:
    return value


@hg.graph
def return_rhs(lhs: hg.TS[int], rhs: hg.TS[int]) -> hg.TS[int]:
    return rhs


@hg.graph
def dict_size(value: hg.TS[int], whole: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
    return hg.len_(whole)


@hg.graph
def generic_size(value: hg.TS[int], whole: hg.TIME_SERIES_TYPE) -> hg.TS[int]:
    return value + hg.len_(whole)


@hg.compute_node
def key_only(key: hg.TS[str]) -> hg.TS[str]:
    return key.value + "!"


@hg.compute_node
def custom_key(symbol: hg.TS[str], value: hg.TS[int]) -> hg.TS[str]:
    return f"{symbol.value}:{value.value}"


@hg.compute_node
def add_one(value: hg.TS[int]) -> hg.TS[int]:
    return value.value + 1


@hg.compute_node
def key_as_value(key: hg.TS[int]) -> hg.TS[int]:
    return key.value + 1


@hg.graph
def bundle_from_scalar(value: hg.TS[int]) -> hg.TSB[Row]:
    return hg.combine[hg.TSB[Row]](value=value, label=hg.const("item"))


@hg.graph
def select_row(lookup: hg.TS[str], rows: hg.TSD[str, hg.TSB[Row]]) -> hg.TSB[Row]:
    return rows[lookup]


@hg.compute_node
def as_ref(value: hg.REF[hg.TIME_SERIES_TYPE]) -> hg.REF[hg.TIME_SERIES_TYPE]:
    return value.value


@hg.compute_node
def window_value(window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[tuple[int, ...]]:
    return tuple(window.value)


@hg.compute_node
def use_window(value: hg.TS[int], window: hg.TSW[int, hg.WINDOW_SIZE, hg.WINDOW_SIZE_MIN]) -> hg.TS[tuple[int, ...]]:
    return (value.value,) + tuple(window.value)


@hg.graph
def child_window(value: hg.TS[int]) -> hg.TSW[int, 3, 1]:
    return hg.to_window(value, 3, 1)


@hg.sink_node
def file_sink(key: hg.TS[str], value: hg.TS[int], prefix: str = "tick"):
    # One file per key means workers never race over a shared append handle.
    path = Path(os.environ["HGRAPH_DMAP_SHAPES_SINK_DIR"]) / f"{key.value}.jsonl"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps([prefix, value.value]) + "\n")


@pytest.fixture(params=["local", "in_process", "process"])
def mapper(request):
    if request.param == "local":
        return hg.map_

    def apply(child, *args, **kwargs):
        return hg.dmap_(child, *args, __workers__=3,
                        in_process=request.param == "in_process", **kwargs)
    return apply


def test_multiple_named_inputs_and_scalar_config(mapper):
    @hg.graph
    def app(lhs: hg.TSD[str, hg.TS[int]], rhs: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(add_config, rhs=rhs, lhs=lhs, factor=3, __label__="configured")
    assert hg.eval_node(app,
        [{"a": 1, "b": 2}, {"b": 4}, {"a": hg.REMOVE}, {"a": 7}],
        [{"a": 10}, {"b": 20}, None, None],
    ) == [{"a": 33}, {"b": 72}, None, {"a": 51}]


def test_positional_inputs_and_default_scalar(mapper):
    @hg.graph
    def app(lhs: hg.TSD[str, hg.TS[int]], rhs: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(add_config, lhs, rhs)
    assert hg.eval_node(app, [{"a": 1, "b": 2}, None, {"c": 3}], [10, 20, None]) == [
        {"a": 22, "b": 24}, {"a": 42, "b": 44}, {"c": 46}]


def test_no_key_excludes_otherwise_independent_outputs(mapper):
    @hg.graph
    def app(keys: hg.TSD[str, hg.TS[int]], values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(return_rhs, keys, hg.no_key(values))
    assert hg.eval_node(app,
        [{"x": 1, "y": 2}, {"x": hg.REMOVE}, {"z": 3}],
        [{"x": 10, "z": 30}, {"z": 40}, None],
    ) == [{"x": 10}, {"x": hg.REMOVE}, {"z": 40}]


def test_pass_through_retains_whole_dictionary(mapper):
    @hg.graph
    def app(keys: hg.TSD[str, hg.TS[int]], whole: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(dict_size, keys, hg.pass_through(whole))
    assert hg.eval_node(app,
        [{"a": 1, "b": 2}, None, {"a": hg.REMOVE, "c": 3}],
        [{"p": 10, "q": 20}, {"p": hg.REMOVE}, None],
    ) == [{"a": 2, "b": 2}, {"a": 1, "b": 1}, {"a": hg.REMOVE, "c": 1}]


def test_different_key_generic_dictionary_is_broadcast(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], whole: hg.TSD[int, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(generic_size, values, whole)
    assert hg.eval_node(app, [{"a": 1, "b": 2}, None], [{1: 10}, {2: 20}]) == [
        {"a": 2, "b": 3}, {"a": 3, "b": 4}]


def test_explicit_keys_include_invalid_children_then_supply_values(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], keys: hg.TSS[str]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(add_one, values, __keys__=keys)
    assert hg.eval_node(app,
        [{"a": 1, "ignored": 9}, {"late": 3}, None, {"a": 7}],
        [{"a", "late"}, None, {hg.Removed("a")}, {"a"}],
    ) == [{"a": 2}, {"late": 4}, {"a": hg.REMOVE}, {"a": 8}]


def test_key_only_map(mapper):
    @hg.graph
    def app(keys: hg.TSS[str]) -> hg.TSD[str, hg.TS[str]]:
        return mapper(key_only, __keys__=keys)
    assert hg.eval_node(app, [{"a", "b"}, {hg.Removed("a")}, {"a"}]) == [
        {"a": "a!", "b": "b!"}, {"a": hg.REMOVE}, {"a": "a!"}]


def test_custom_key_name(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[str]]:
        return mapper(custom_key, values, __key_arg__="symbol")
    assert hg.eval_node(app, [{"a": 2, "b": 3}]) == [{"a": "a:2", "b": "b:3"}]


@pytest.mark.parametrize("size,events,expected", [
    (2, [(1, 2), (None, 3), None, (4, None)], [{0: 2, 1: 3}, {1: 4}, None, {0: 5}]),
    (-1, [{0: 1}, {2: 3}, {0: 4}, None], [{0: 2}, {2: 4}, {0: 5}, None]),
])
def test_fixed_and_dynamic_list_mapping(mapper, size, events, expected):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[size]]) -> hg.TSL[hg.TS[int], hg.Size[size]]:
        return mapper(add_one, values)
    assert hg.eval_node(app, events) == expected


@pytest.mark.parametrize("element_type,events", [
    (hg.TSS[str], [{"a": {"x", "y"}}, {"a": {hg.Removed("x"), "z"}}, {"a": hg.REMOVE}]),
    (hg.TSD[str, hg.TS[int]], [{"a": {"x": 1, "y": 2}}, {"a": {"x": hg.REMOVE}}, {"a": hg.REMOVE}]),
    (hg.TSL[hg.TS[int], hg.Size[2]], [{"a": {0: 1}}, {"a": {1: 2}}, {"a": hg.REMOVE}]),
    (hg.TSL[hg.TS[int], hg.Size[-1]], [{"a": {0: 1}}, {"a": {3: 2}}, {"a": hg.REMOVE}]),
    (hg.TSB[Row], [{"a": {"value": 1}}, {"a": {"label": "one"}}, {"a": hg.REMOVE}]),
    (hg.TSB[Nested], [{"a": {"row": {"value": 1}, "keys": {"x"}, "values": {"p": 2}, "pair": {0: 3}}},
                     {"a": {"row": {"label": "one"}, "keys": {hg.Removed("x")}, "values": {"p": hg.REMOVE}, "pair": {1: 4}}},
                     {"a": hg.REMOVE}]),
])
def test_structural_input_output_deltas(mapper, element_type, events):
    @hg.graph
    def app(values: hg.TSD[str, element_type]) -> hg.TSD[str, element_type]:
        return mapper(identity, values)
    @hg.graph
    def oracle(values: hg.TSD[str, element_type]) -> hg.TSD[str, element_type]:
        return hg.map_(identity, values)
    expected = hg.eval_node(oracle, events)
    assert expected[0]
    assert expected[-1] == {"a": hg.REMOVE}
    assert hg.eval_node(app, events) == expected


def test_bundle_output_partial_updates(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TSB[Row]]:
        return mapper(bundle_from_scalar, values)
    assert hg.eval_node(app, [{"a": 1}, {"a": 2}, {"a": hg.REMOVE}]) == [
        {"a": {"value": 1, "label": "item"}}, {"a": {"value": 2}}, {"a": hg.REMOVE}]


def test_reference_input_is_materialized_and_tracks_target_ticks(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(identity, as_ref(values))
    events = [{"a": 1}, {"a": 2, "b": 3}, {"a": hg.REMOVE}, {"b": 4}]
    assert hg.eval_node(app, events) == events


def test_reference_output_selection_rebinding_and_removal(mapper):
    @hg.graph
    def app(lookups: hg.TSD[str, hg.TS[str]], rows: hg.TSD[str, hg.TSB[Row]]) -> hg.TSD[str, hg.TSB[Row]]:
        return mapper(select_row, lookups, hg.pass_through(rows))
    lookups = [{"left": "a", "right": "b"}, {"left": "b"}, {"right": hg.REMOVE}, None]
    rows = [{"a": {"value": 1, "label": "one"}, "b": {"value": 2, "label": "two"}},
            None, {"b": {"value": 3}}, {"b": {"label": "new"}}]
    assert hg.eval_node(app, lookups, rows) == [
        {"left": {"value": 1, "label": "one"}, "right": {"value": 2, "label": "two"}},
        {"left": {"value": 2, "label": "two"}},
        {"left": {"value": 3}, "right": hg.REMOVE},
        {"left": {"label": "new"}},
    ]


def test_window_input_is_broadcast_with_history_and_eviction(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], ticks: hg.TS[int]) -> hg.TSD[str, hg.TS[tuple[int, ...]]]:
        return mapper(use_window, values, hg.to_window(ticks, 3, 1))
    assert hg.eval_node(app, [{"a": 10}, None, {"b": 20}, None], [1, 2, 3, 4]) == [
        {"a": (10, 1)}, {"a": (10, 1, 2)}, {"a": (10, 1, 2, 3), "b": (20, 1, 2, 3)},
        {"a": (10, 2, 3, 4), "b": (20, 2, 3, 4)}]


def test_window_output_can_be_consumed_in_parent(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[tuple[int, ...]]]:
        windows = mapper(child_window, values)
        return hg.map_(window_value, windows)
    assert hg.eval_node(app, [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}]) == [
        {"a": (1,)}, {"a": (1, 2)}, {"a": (1, 2, 3)}, {"a": (2, 3, 4)}]


def test_outputless_sink_and_scalar_parameter(mapper, tmp_path, monkeypatch):
    monkeypatch.setenv("HGRAPH_DMAP_SHAPES_SINK_DIR", str(tmp_path))
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]):
        mapper(file_sink, values, prefix="seen")
    hg.eval_node(app, [{"a": 1, "b": 2}, {"a": 3}, {"b": hg.REMOVE}])
    assert [json.loads(line) for line in (tmp_path / "a.jsonl").read_text().splitlines()] == [
        ["seen", 1], ["seen", 3]]
    assert [json.loads(line) for line in (tmp_path / "b.jsonl").read_text().splitlines()] == [["seen", 2]]


class Side(Enum):
    BUY = 1
    SELL = 2


@dataclass(frozen=True)
class Quote(hg.CompoundScalar):
    price: float
    label: str


@pytest.mark.parametrize("scalar_type,value", [
    (bool, False), (int, -(2**40)), (float, 3.125), (str, "\0unicode λ"),
    (bytes, b"\x00\xffbinary"), (date, date(2024, 3, 4)),
    (time, time(12, 34, 56, 789)), (datetime, datetime(2024, 3, 4, 5, 6, 7, 89)),
    (timedelta, timedelta(days=-2, microseconds=13)),
    (tuple[int, str], (7, "seven")), (tuple[int, ...], (1, 2, 3)),
    (frozenset[str], frozenset({"a", "b"})), (dict[str, int], {"a": 1, "b": 2}),
    (Side, Side.SELL), (Quote, Quote(3.25, "ask")),
    (hg.CivilDateTime, hg.CivilDateTime(date(2024, 3, 4), time(12, 30))),
    (hg.Period, hg.Period(months=2, days=-3)),
    (hg.ZoneId, hg.ZoneId("Europe/London")),
    (hg.InstantRange, hg.InstantRange(datetime(2024, 3, 4), datetime(2024, 3, 5))),
    (hg.CivilDateRange, hg.CivilDateRange(date(2024, 3, 4), date(2024, 3, 5))),
])
def test_scalar_schema_families(mapper, scalar_type, value):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[scalar_type]]) -> hg.TSD[str, hg.TS[scalar_type]]:
        return mapper(identity, values)
    assert hg.eval_node(app, [{"a": value}, None, {"a": hg.REMOVE}, {"a": value}]) == [
        {"a": value}, None, {"a": hg.REMOVE}, {"a": value}]


def test_explicitly_disabled_key_injection(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(key_as_value, values, __key_arg__="")
    assert hg.eval_node(app, [{"a": 4}]) == [{"a": 5}]


@hg.graph
def bundle_parameters(value: hg.TS[int], params: hg.TSB[hg.TS_SCHEMA]) -> hg.TS[int]:
    resolved = hg.dereference(params)
    return hg.if_then_else(resolved.flag, value + resolved.limit, value - resolved.limit)


def test_structural_keyword_mapping_is_prepared_like_map(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], flag: hg.TS[bool], limit: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(bundle_parameters, value=values, params={"flag": flag, "limit": limit})
    assert hg.eval_node(app, [{"a": 2, "b": 3}, None], [True, False], [10, 5]) == [
        {"a": 12, "b": 13}, {"a": -3, "b": -2}]


def test_passive_broadcast_matches_ordinary_map(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], offset: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(add_config, values, hg.passive(offset), factor=1)
    assert hg.eval_node(app, [{"a": 1}, None, {"a": 2}], [10, 20, None]) == [
        {"a": 11}, {"a": 21}, {"a": 22}]


def test_native_operator_child(mapper):
    @hg.graph
    def app(lhs: hg.TSD[str, hg.TS[int]], rhs: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(hg.add_, lhs, rhs)
    assert hg.eval_node(app, [{"a": 1}, {"a": 3}], [{"a": 2}, None]) == [{"a": 3}, {"a": 5}]


def test_typed_frame_preserves_rows_and_metadata(mapper):
    import pyarrow as pa
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[hg.Frame[Quote]]]) -> hg.TSD[str, hg.TS[hg.Frame[Quote]]]:
        return mapper(identity, values)
    table = pa.table({"price": [3.25, 4.5], "label": ["ask", "bid"]},
                     metadata={b"source": b"distributed-test"})
    actual = hg.eval_node(app, [{"a": table}, None, {"a": hg.REMOVE}])
    assert actual[0]["a"].equals(table, check_metadata=True)
    assert actual[1:] == [None, {"a": hg.REMOVE}]


def test_zoned_datetime_scalar(mapper):
    with hg.GlobalContext(hg.GlobalState()):
        hg.set_time_zone_provider()
        value = hg.eval_node(hg.at_zone, [datetime(2024, 3, 4, 12)], [hg.ZoneId("Europe/London")])[0]
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[hg.ZonedDateTime]]) -> hg.TSD[str, hg.TS[hg.ZonedDateTime]]:
        return mapper(identity, values)
    assert hg.eval_node(app, [{"a": value}]) == [{"a": value}]


@hg.compute_node
def add_index(ndx: hg.TS[int], value: hg.TS[int]) -> hg.TS[int]:
    return ndx.value * 100 + value.value


@hg.graph
def whole_list(value: hg.TS[int], whole: hg.TSL[hg.TS[int], hg.SIZE]) -> hg.TS[int]:
    return value + hg.sum_(whole)


@pytest.mark.parametrize("size", [2, -1])
def test_list_pairing_and_broadcast(mapper, size):
    @hg.graph
    def app(lhs: hg.TSL[hg.TS[int], hg.Size[size]], rhs: hg.TSL[hg.TS[int], hg.Size[size]], offset: hg.TS[int]) -> hg.TSL[hg.TS[int], hg.Size[size]]:
        paired = mapper(add_config, lhs, rhs, factor=1)
        return mapper(add_config, paired, offset, factor=1)
    assert hg.eval_node(app, [{0: 1, 1: 2}, {1: 4}, None],
                        [{0: 10, 1: 20}, None, {0: 30}], [100, None, 200]) == [
        {0: 111, 1: 122}, {1: 124}, {0: 231, 1: 224}]


@pytest.mark.parametrize("size", [2, -1])
def test_list_index_injection(mapper, size):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[size]]) -> hg.TSL[hg.TS[int], hg.Size[size]]:
        return mapper(add_index, values)
    assert hg.eval_node(app, [{0: 1, 1: 2}, {1: 3}]) == [{0: 1, 1: 102}, {1: 103}]


@pytest.mark.parametrize("size", [2, -1])
def test_list_pass_through_broadcasts_whole_collection(mapper, size):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[size]], whole: hg.TSL[hg.TS[int], hg.Size[size]]) -> hg.TSL[hg.TS[int], hg.Size[size]]:
        return mapper(whole_list, values, hg.pass_through(whole))
    assert hg.eval_node(app, [{0: 1, 1: 2}, None], [{0: 10, 1: 20}, {0: 30}]) == [
        {0: 31, 1: 32}, {0: 51, 1: 52}]


@hg.compute_node
def configured_scalar_types(value: hg.TS[int], quote: Quote, side: Side,
                            offsets: tuple[int, ...] = (1, 2)) -> hg.TS[str]:
    return f"{side.name}:{quote.label}:{value.value + quote.price + sum(offsets)}"


@hg.graph
def configured_type(value: hg.TS[int], target: type = float) -> hg.TS[float]:
    return hg.convert[hg.TS[target]](value)


def test_scalar_configuration_schema_families(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[str]]:
        return mapper(configured_scalar_types, values,
                      quote=Quote(3.25, "ask"), side=Side.SELL, offsets=(4, 5))
    assert hg.eval_node(app, [{"a": 2}]) == [{"a": "SELL:ask:14.25"}]


def test_type_configuration_is_imported_in_worker(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[float]]:
        return mapper(configured_type, values, target=float)
    actual = hg.eval_node(app, [{"a": 2}])
    assert actual == [{"a": 2.0}]
    assert type(actual[0]["a"]) is float


def test_specialized_native_operator_child(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[float]]) -> hg.TSD[str, hg.TS[float]]:
        return mapper(hg.abs_[hg.TS[float]], values)
    actual = hg.eval_node(app, [{"a": -2.5}])
    assert actual == [{"a": 2.5}]
    assert type(actual[0]["a"]) is float


def test_shared_compound_scalar(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[hg.Shared[Quote]]]) -> hg.TSD[str, hg.TS[hg.Shared[Quote]]]:
        return mapper(identity, values)
    value = Quote(1.5, "shared")
    assert hg.eval_node(app, [{"a": value}, None, {"a": hg.REMOVE}]) == [
        {"a": value}, None, {"a": hg.REMOVE}]


@pytest.mark.parametrize("size", [3, -1])
def test_array_retains_logical_shape_and_values(mapper, size):
    import numpy as np
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[hg.Array[int, hg.Size[size]]]]) -> hg.TSD[str, hg.TS[hg.Array[int, hg.Size[size]]]]:
        return mapper(identity, values)
    actual = hg.eval_node(app, [{"a": np.array([1])}, {"a": np.array([1, 2, 3])}])
    assert np.array_equal(actual[0]["a"], np.array([1]))
    assert np.array_equal(actual[1]["a"], np.array([1, 2, 3]))


def test_series_preserves_nulls_and_logical_type(mapper):
    import pyarrow as pa
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[hg.Series[int]]]) -> hg.TSD[str, hg.TS[hg.Series[int]]]:
        return mapper(identity, values)
    value = pa.array([1, None, 3], type=pa.int64())
    actual = hg.eval_node(app, [{"a": value}])
    assert actual[0]["a"].equals(value)


@dataclass(frozen=True)
class TaggedQuote(Quote):
    venue: str


def test_polymorphic_scalar_retains_derived_type(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[Quote]]) -> hg.TSD[str, hg.TS[Quote]]:
        return mapper(identity, values)
    value = TaggedQuote(2.5, "ask", "exchange")
    actual = hg.eval_node(app, [{"a": value}])
    assert actual == [{"a": value}]
    assert type(actual[0]["a"]) is TaggedQuote


@hg.compute_node
def zone_key_name(key: hg.TS[hg.ZoneId], value: hg.TS[int]) -> hg.TS[str]:
    return f"{key.value}:{value.value}"


def test_zone_id_keys_partition_by_semantics_across_processes(mapper, monkeypatch):
    monkeypatch.setenv("HGRAPH_DMAP_SHAPES_PARENT_PID", str(os.getpid()))
    keys = [hg.ZoneId(name) for name in (
        "Europe/London", "America/New_York", "Asia/Dubai", "Australia/Sydney", "Africa/Johannesburg")]
    @hg.graph
    def app(values: hg.TSD[hg.ZoneId, hg.TS[int]]) -> hg.TSD[hg.ZoneId, hg.TS[str]]:
        return mapper(zone_key_name, values)
    events = [dict(zip(keys, range(1, len(keys) + 1))), {keys[0]: hg.REMOVE}, {keys[0]: 9}]
    assert hg.eval_node(app, events) == [
        {key: f"{key}:{value}" for key, value in events[0].items()},
        {keys[0]: hg.REMOVE}, {keys[0]: f"{keys[0]}:9"}]


@hg.compute_node
def signal_ticks(value: hg.TS[int], pulse: hg.SIGNAL,
                 state: hg.STATE = None) -> hg.TS[int]:
    state.ticks = getattr(state, "ticks", 0) + int(pulse.modified)
    return value.value + state.ticks


def test_signal_broadcast_preserves_tick_activation(mapper):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]], pulse: hg.SIGNAL) -> hg.TSD[str, hg.TS[int]]:
        return mapper(signal_ticks, values, pulse)
    assert hg.eval_node(app, [{"a": 10}, None, {"b": 20}], [True, True, None]) == [
        {"a": 11}, {"a": 12}, {"b": 21}]


def test_duration_window_history_and_clear_broadcast(mapper):
    def run(apply):
        @hg.graph
        def app(values: hg.TSD[str, hg.TS[int]], ticks: hg.TS[int], reset: hg.SIGNAL) -> hg.TSD[str, hg.TS[tuple[int, ...]]]:
            window = hg.to_window(ticks, timedelta(microseconds=3), timedelta(microseconds=1), reset=reset)
            return apply(use_window, values, window)
        return hg.eval_node(app, [{"a": 10}, None, {"b": 20}, None, None],
                            [1, 2, 3, None, 4], [None, None, None, True, None])
    expected = run(hg.map_)
    assert expected[-1] == {"a": (10, 4), "b": (20, 4)}
    assert run(mapper) == expected


@hg.sink_node
def indexed_file_sink(ndx: hg.TS[int], value: hg.TS[int]):
    path = Path(os.environ["HGRAPH_DMAP_SHAPES_SINK_DIR"]) / f"{ndx.value}.jsonl"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value.value) + "\n")


@pytest.mark.parametrize("size", [2, -1])
def test_outputless_list_sinks_run_once_per_index(mapper, size, tmp_path, monkeypatch):
    monkeypatch.setenv("HGRAPH_DMAP_SHAPES_SINK_DIR", str(tmp_path))
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[size]]):
        mapper(indexed_file_sink, values)
    hg.eval_node(app, [{0: 1, 1: 2}, {1: 3}])
    assert [json.loads(line) for line in (tmp_path / "0.jsonl").read_text().splitlines()] == [1]
    assert [json.loads(line) for line in (tmp_path / "1.jsonl").read_text().splitlines()] == [2, 3]


@hg.graph
def constant_child() -> hg.TS[int]:
    return hg.const(7)


def test_zero_argument_child_uses_explicit_keys(mapper):
    @hg.graph
    def app(keys: hg.TSS[str]) -> hg.TSD[str, hg.TS[int]]:
        return mapper(constant_child, __keys__=keys)
    assert hg.eval_node(app, [set(), {"a"}, {hg.Removed("a")}, {"b"}]) == [
        {}, {"a": 7}, {"a": hg.REMOVE}, {"b": 7}]


def test_empty_fixed_list(mapper):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[0]]) -> hg.TSL[hg.TS[int], hg.Size[0]]:
        return mapper(add_one, values)
    @hg.graph
    def oracle(values: hg.TSL[hg.TS[int], hg.Size[0]]) -> hg.TSL[hg.TS[int], hg.Size[0]]:
        return hg.map_(add_one, values)
    assert hg.eval_node(app, [None]) == hg.eval_node(oracle, [None])


@hg.compute_node
def accumulating_child(value: hg.TS[int], state: hg.STATE = None) -> hg.TS[int]:
    state.total = getattr(state, "total", 0) + value.value
    return state.total


def test_dynamic_list_truncation_recreates_child_state(mapper):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[-1]]) -> hg.TSL[hg.TS[int], hg.Size[-1]]:
        return mapper(accumulating_child, values)
    assert hg.eval_node(app, [{0: 1, 1: 2, 2: 3}, {1: hg.REMOVE}, {1: 4, 2: 5}, {0: 6}]) == [
        {0: 1, 1: 2, 2: 3}, {1: hg.REMOVE, 2: hg.REMOVE}, {1: 4, 2: 5}, {0: 7}]


@hg.compute_node
def initially_invalid_member(event: hg.TS[int], _output: hg.TSD = None) -> hg.TSD[str, hg.TS[int]]:
    if event.value == 1:
        _output.get_or_create("a")
    elif event.value == 2:
        _output.get_or_create("a").value = 3
    else:
        del _output["a"]


@hg.compute_node
def count_members(key: hg.TS[str], whole: hg.TSD[str, hg.TS[int]]) -> hg.TS[int]:
    return len(whole)


def test_pass_through_preserves_invalid_dictionary_membership(mapper):
    @hg.graph
    def app(event: hg.TS[int]) -> hg.TSD[str, hg.TS[int]]:
        whole = initially_invalid_member(event)
        keys = hg.const(frozenset({"only"}), tp=hg.TSS[str])
        return mapper(count_members, hg.pass_through(whole), __keys__=keys)
    assert hg.eval_node(app, [1, 2, 3]) == [{"only": 1}, {"only": 1}, {"only": 0}]


_captured_outer_port = None


@hg.push_queue(hg.TS[int])
def forbidden_push_source(sender):
    raise AssertionError("a rejected distributed child must never start")


@hg.graph
def child_with_push(value: hg.TS[int]) -> hg.TS[int]:
    return value + forbidden_push_source()


@hg.graph
def child_with_nested_push(value: hg.TS[int]) -> hg.TS[int]:
    return hg.switch_(hg.const(True), {True: child_with_push}, value)


@hg.graph
def child_with_captured_port(value: hg.TS[int]) -> hg.TS[int]:
    return value + _captured_outer_port


@hg.graph
def child_with_parent_context(value: hg.TS[int]) -> hg.TS[int]:
    return value + hg.get_context("distributed_test_offset", hg.TS[int])


@hg.reference_service
def parent_only_service() -> hg.TS[int]: ...


@hg.graph
def child_with_parent_service(value: hg.TS[int]) -> hg.TS[int]:
    return value + parent_only_service(path="parent-only")


@pytest.mark.parametrize("in_process", [True, False])
@pytest.mark.parametrize("child,cause", [
    (child_with_push, "push source"),
    (child_with_nested_push, "push source"),
    (child_with_captured_port, "different wiring"),
    (child_with_parent_context, "different wiring"),
    (child_with_parent_service, "service"),
])
def test_disallowed_external_dependencies_fail_before_launch(child, cause, in_process, monkeypatch):
    import sys
    # A process launch would produce an executable error rather than the
    # precise native wiring diagnostic asserted below.
    monkeypatch.setattr(sys, "executable", "/nonexistent/distributed-test-worker")
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        offset = hg.const(10)
        monkeypatch.setattr(sys.modules[__name__], "_captured_outer_port", offset)
        with hg.context("distributed_test_offset", offset):
            return hg.dmap_(child, values, in_process=in_process)
    with pytest.raises(ValueError) as raised:
        hg.eval_node(app, [{"a": 1}])
    message = str(raised.value)
    assert "dmap_: the child cannot be wired as an isolated worker" in message
    assert cause in message.partition("Wiring reported:")[2]


@hg.compute_node
def isolated_worker_state(value: hg.TS[int], global_state: hg.GlobalState = None) -> hg.TS[int]:
    assert "parent_only" not in global_state
    assert global_state["worker_configured"] is True
    global_state["out"] = global_state.get("out", 0) + value.value
    return global_state["out"]


@hg.graph
def configure_isolated_worker(value: hg.TS[int], global_state: hg.GlobalState = None) -> hg.TS[int]:
    global _captured_worker_state
    _captured_worker_state = global_state
    assert "parent_only" not in global_state
    global_state["worker_configured"] = True
    return isolated_worker_state(value)


@pytest.mark.parametrize("in_process", [True, False])
def test_worker_global_state_is_isolated_and_staging_does_not_collide(in_process):
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.dmap_(configure_isolated_worker, values, __workers__=3, in_process=in_process)
    state = hg.GlobalState()
    state["parent_only"] = 123
    state["out"] = 99
    with hg.GlobalContext(state):
        assert hg.eval_node(app, [{"a": 2}, {"a": 3}]) == [{"a": 2}, {"a": 5}]
    assert state["out"] == 99
    assert "worker_configured" not in state
    with pytest.raises(RuntimeError, match="outside"):
        _captured_worker_state.get("worker_configured")


@hg.compute_node
def read_worker_configuration(value: hg.TS[int], global_state: hg.GlobalState = None) -> hg.TS[int]:
    assert global_state["wiring_count"] == 1
    return value.value


@hg.graph
def configure_worker_once(value: hg.TS[int], global_state: hg.GlobalState = None) -> hg.TS[int]:
    global_state["wiring_count"] = global_state.get("wiring_count", 0) + 1
    assert global_state["wiring_count"] == 1
    return read_worker_configuration(value)


@pytest.mark.parametrize("in_process", [True, False])
def test_each_worker_composes_configuration_once(in_process):
    @hg.graph
    def app(values: hg.TSL[hg.TS[int], hg.Size[-1]]) -> hg.TSL[hg.TS[int], hg.Size[-1]]:
        return hg.dmap_(configure_worker_once, values, __workers__=3, in_process=in_process)
    # Six dynamic-list indices cover all three worker partitions. Each worker
    # compiles one child template whose configuration survives subsequent ticks.
    initial = {i: i + 1 for i in range(6)}
    update = {i: i + 100 for i in range(6)}
    state = hg.GlobalState()
    with hg.GlobalContext(state):
        assert hg.eval_node(app, [initial, update]) == [initial, update]
    assert "wiring_count" not in state


@hg.compute_node
def bootstrap_payload_size(value: hg.TS[int], payload: str, last_import_path: str) -> hg.TS[int]:
    import sys
    assert sys.path[-1] == last_import_path
    return value.value + len(payload)


def test_large_scalar_and_import_paths_use_bootstrap_channel(monkeypatch):
    import sys
    # Both fields independently exceed Windows' command-line limit; the scalar
    # also exceeds the per-argument limit commonly enforced by POSIX kernels.
    payload = "configuration-" * 25_000
    paths = [f"/unused-dmap-import/{index}/" + "p" * 230 for index in range(1000)]
    monkeypatch.setattr(sys, "path", [*sys.path, *paths])
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.dmap_(bootstrap_payload_size, values, payload=payload,
                        last_import_path=paths[-1], __workers__=1)
    assert hg.eval_node(app, [{"a": 1}, {"a": 2}]) == [
        {"a": len(payload) + 1}, {"a": len(payload) + 2}]


@pytest.mark.parametrize("timeout", [True, False, 0, -1, float("inf"), float("nan"),
                                    86_401, 10 ** 1000, "60", None])
def test_worker_timeout_requires_finite_positive_seconds(timeout):
    with pytest.raises(ValueError, match="__worker_timeout__"):
        hg.dmap_(add_one, __worker_timeout__=timeout)


@hg.compute_node
def blocked_worker(value: hg.TS[int]) -> hg.TS[int]:
    import time as wall_time
    wall_time.sleep(10)
    return value.value


def test_process_worker_deadline_fails_the_run():
    @hg.graph
    def app(values: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
        return hg.dmap_(blocked_worker, values, __workers__=1, __worker_timeout__=0.25)
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        hg.eval_node(app, [{"a": 1}])
