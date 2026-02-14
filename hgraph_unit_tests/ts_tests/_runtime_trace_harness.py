from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any

from hgraph import (
    DEFAULT,
    REF,
    REMOVE,
    Removed,
    Size,
    TS,
    TSD,
    TSB,
    TSL,
    TSS,
    TimeSeriesReference,
    TimeSeriesSchema,
    combine,
    compute_node,
    contains_,
    count,
    const,
    default,
    filter_,
    format_,
    generator,
    graph,
    EvaluationClock,
    if_,
    if_then_else,
    last_modified_time,
    map_,
    mesh_,
    MIN_TD,
    modified,
    nothing,
    pass_through,
    reduce,
    reduce_tsd_of_bundles_with_race,
    reduce_tsd_with_race,
    switch_,
    valid,
)
from hgraph.test import eval_node


def _stable_key(value: Any) -> str:
    return json.dumps(_canonicalize(value), sort_keys=True)


def _canonicalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_canonicalize(v) for v in value]
    if isinstance(value, dict):
        items = [[_canonicalize(k), _canonicalize(v)] for k, v in value.items()]
        items.sort(key=lambda kv: _stable_key(kv[0]))
        return {"__dict__": items}
    if isinstance(value, (set, frozenset)):
        out = [_canonicalize(v) for v in value]
        out.sort(key=_stable_key)
        return {"__set__": out}
    if value is REMOVE:
        return {"__sentinel__": "REMOVE"}
    if is_dataclass(value):
        return {"__dataclass__": type(value).__name__, "fields": _canonicalize(asdict(value))}
    if hasattr(value, "added") and hasattr(value, "removed"):
        added = [_canonicalize(v) for v in value.added]
        removed = [_canonicalize(v) for v in value.removed]
        added.sort(key=_stable_key)
        removed.sort(key=_stable_key)
        return {"__delta_set__": {"added": added, "removed": removed}}
    return repr(value)


def _normalize_modified_trace_teardown(
    modified_trace: Any, value_trace: Any, valid_trace: Any, lmt_trace: Any
) -> Any:
    """
    Normalize a known teardown artifact where one runtime emits a final trailing
    `False` tick for `modified` after value/valid/lmt traces have completed.
    """
    if not all(isinstance(trace, list) for trace in (modified_trace, value_trace, valid_trace, lmt_trace)):
        return modified_trace

    base_lengths = {len(value_trace), len(valid_trace), len(lmt_trace)}
    if len(base_lengths) != 1:
        return modified_trace

    expected_len = next(iter(base_lengths))
    if len(modified_trace) == expected_len + 1 and modified_trace and modified_trace[-1] is False:
        return modified_trace[:-1]
    return modified_trace


def _collect_ts_trace(fn, valid_fn, modified_fn, lmt_fn, **inputs) -> dict[str, Any]:
    value_trace = eval_node(fn, **inputs)
    valid_trace = eval_node(valid_fn, **inputs)
    modified_trace = eval_node(modified_fn, **inputs)
    lmt_trace = eval_node(lmt_fn, **inputs)
    modified_trace = _normalize_modified_trace_teardown(modified_trace, value_trace, valid_trace, lmt_trace)

    return {
        "value": _canonicalize(value_trace),
        "valid": _canonicalize(valid_trace),
        "modified": _canonicalize(modified_trace),
        "last_modified_time": _canonicalize(lmt_trace),
    }


class _RaceBundleSchema(TimeSeriesSchema):
    a: TS[int]
    b: TS[int]


class _RaceBundleSwitchSchema(_RaceBundleSchema):
    free: TS[bool]
    cond: TS[bool]


class _NestedBundleSchema(TimeSeriesSchema):
    a: TS[int]
    b: TSD[int, TSD[int, TS[int]]]


def _scenario_nested_map_switch_reduce() -> dict[str, Any]:
    class AB(TimeSeriesSchema):
        a: TS[int]
        b: TS[int]

    @graph
    def nested_flow(n: TSD[int, TS[int]]) -> TS[int]:
        refs = map_(
            lambda key, value: combine[TSB[AB]](a=value, b=switch_(value, {DEFAULT: lambda v: v + 1}, value)),
            n,
        )
        return reduce(lambda x, y: combine[TSB[AB]](a=x.a + y.a, b=x.b + y.b), refs, combine[TSB[AB]](a=0, b=0)).b

    @graph
    def nested_flow_valid(n: TSD[int, TS[int]]) -> TS[bool]:
        return valid(nested_flow(n))

    @graph
    def nested_flow_modified(n: TSD[int, TS[int]]) -> TS[bool]:
        return modified(nested_flow(n))

    @graph
    def nested_flow_lmt(n: TSD[int, TS[int]]) -> TS[datetime]:
        return last_modified_time(nested_flow(n))

    return _collect_ts_trace(
        nested_flow,
        nested_flow_valid,
        nested_flow_modified,
        nested_flow_lmt,
        n=[
            {},
            {1: 1},
            {2: 2},
            {1: 2},
            {2: 3},
            {1: REMOVE},
            {2: REMOVE},
            {3: 10},
            {3: 11, 4: 5},
            {4: REMOVE},
        ],
    )


@compute_node
def _make_ref_for_race(ts: TS[int], ref: REF[TS[int]], ref2: REF[TS[int]]) -> REF[TS[int]]:
    match ts.value:
        case 0:
            return TimeSeriesReference.make()
        case -1:
            return ref2.value
        case _:
            return ref.value


def _scenario_ref_race_tsd_to_ts() -> dict[str, Any]:
    @graph
    def ref_race(tsd: TSD[int, TS[int]], ts: TS[int]) -> REF[TS[int]]:
        refs = map_(
            _make_ref_for_race,
            tsd,
            tsd,
            switch_(ts, {-1: lambda: const(-1), DEFAULT: lambda: nothing(TS[int])}),
        )
        return reduce_tsd_with_race(tsd=refs)

    @graph
    def ref_race_valid(tsd: TSD[int, TS[int]], ts: TS[int]) -> TS[bool]:
        return valid(ref_race(tsd, ts))

    @graph
    def ref_race_modified(tsd: TSD[int, TS[int]], ts: TS[int]) -> TS[bool]:
        return modified(ref_race(tsd, ts))

    @graph
    def ref_race_lmt(tsd: TSD[int, TS[int]], ts: TS[int]) -> TS[datetime]:
        return last_modified_time(ref_race(tsd, ts))

    return _collect_ts_trace(
        ref_race,
        ref_race_valid,
        ref_race_modified,
        ref_race_lmt,
        tsd=[
            {1: 0, 2: -1},
            {2: 0},
            {1: 1},
            {2: 2, 3: 3},
            None,
            {1: REMOVE},
            {2: 0},
        ],
        ts=[0, None, None, None, -1],
    )


@compute_node
def _select_ref(index: TS[int], ts: TSL[REF[TS[int]], Size[2]]) -> REF[TS[int]]:
    from typing import cast

    return cast(REF, ts[index.value].value)


@compute_node
def _select_tsl_ref(
    index: TS[int], ts: TSL[REF[TSL[TS[int], Size[2]]], Size[2]]
) -> REF[TSL[TS[int], Size[2]]]:
    from typing import cast

    return cast(REF[TSL[TS[int], Size[2]]], ts[index.value].value)


def _scenario_ref_toggle_to_ts() -> dict[str, Any]:
    @graph
    def ref_toggle(index: TS[int], ts1: TS[int], ts2: TS[int]) -> REF[TS[int]]:
        return _select_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def ref_toggle_valid(index: TS[int], ts1: TS[int], ts2: TS[int]) -> TS[bool]:
        return valid(ref_toggle(index, ts1, ts2))

    @graph
    def ref_toggle_modified(index: TS[int], ts1: TS[int], ts2: TS[int]) -> TS[bool]:
        return modified(ref_toggle(index, ts1, ts2))

    @graph
    def ref_toggle_lmt(index: TS[int], ts1: TS[int], ts2: TS[int]) -> TS[datetime]:
        return last_modified_time(ref_toggle(index, ts1, ts2))

    return _collect_ts_trace(
        ref_toggle,
        ref_toggle_valid,
        ref_toggle_modified,
        ref_toggle_lmt,
        index=[0, None, 1, None, 0, 1],
        ts1=[1, 2, 3, None, 5, None],
        ts2=[-1, -2, -3, -4, -5, -6],
    )


def _scenario_ref_merge_from_tsl_input() -> dict[str, Any]:
    @graph
    def merge_from_tsl(index: TS[int], ts: TSL[REF[TS[int]], Size[2]]) -> REF[TS[int]]:
        return _select_ref(index, ts)

    @graph
    def merge_from_tsl_valid(index: TS[int], ts: TSL[REF[TS[int]], Size[2]]) -> TS[bool]:
        return valid(merge_from_tsl(index, ts))

    @graph
    def merge_from_tsl_modified(index: TS[int], ts: TSL[REF[TS[int]], Size[2]]) -> TS[bool]:
        return modified(merge_from_tsl(index, ts))

    @graph
    def merge_from_tsl_lmt(index: TS[int], ts: TSL[REF[TS[int]], Size[2]]) -> TS[datetime]:
        return last_modified_time(merge_from_tsl(index, ts))

    return _collect_ts_trace(
        merge_from_tsl,
        merge_from_tsl_valid,
        merge_from_tsl_modified,
        merge_from_tsl_lmt,
        index=[0, None, 1, None],
        ts=[(1, -1), (2, -2), None, (4, -4)],
    )


def _scenario_ref_tsl_inner_switch() -> dict[str, Any]:
    @graph
    def ref_tsl(index: TS[int], ts1: TSL[TS[int], Size[2]], ts2: TSL[TS[int], Size[2]]) -> REF[TSL[TS[int], Size[2]]]:
        return _select_tsl_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def ref_tsl_valid(index: TS[int], ts1: TSL[TS[int], Size[2]], ts2: TSL[TS[int], Size[2]]) -> TS[bool]:
        return valid(ref_tsl(index, ts1, ts2))

    @graph
    def ref_tsl_modified(index: TS[int], ts1: TSL[TS[int], Size[2]], ts2: TSL[TS[int], Size[2]]) -> TS[bool]:
        return modified(ref_tsl(index, ts1, ts2))

    @graph
    def ref_tsl_lmt(index: TS[int], ts1: TSL[TS[int], Size[2]], ts2: TSL[TS[int], Size[2]]) -> TS[datetime]:
        return last_modified_time(ref_tsl(index, ts1, ts2))

    return _collect_ts_trace(
        ref_tsl,
        ref_tsl_valid,
        ref_tsl_modified,
        ref_tsl_lmt,
        index=[0, None, 1, None],
        ts1=[(1, 1), (2, None), None, (None, 4)],
        ts2=[(-1, -1), (-2, -2), None, (-4, None)],
    )


def _scenario_ref_unbind_active_transition() -> dict[str, Any]:
    @compute_node
    def make_ref(bind: TS[bool], ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if bind.value else TimeSeriesReference.make()

    @compute_node
    def observe_ref(ref: REF[TS[int]]) -> TS[tuple[bool, bool, bool]]:
        value = ref.value
        return ref.valid, ref.modified, value.is_empty if value else True

    @graph
    def ref_unbind(bind: TS[bool], ts: TS[int]) -> TS[tuple[bool, bool, bool]]:
        return observe_ref(make_ref(bind, ts))

    @graph
    def ref_unbind_valid(bind: TS[bool], ts: TS[int]) -> TS[bool]:
        return valid(ref_unbind(bind, ts))

    @graph
    def ref_unbind_modified(bind: TS[bool], ts: TS[int]) -> TS[bool]:
        return modified(ref_unbind(bind, ts))

    @graph
    def ref_unbind_lmt(bind: TS[bool], ts: TS[int]) -> TS[datetime]:
        return last_modified_time(ref_unbind(bind, ts))

    return _collect_ts_trace(
        ref_unbind,
        ref_unbind_valid,
        ref_unbind_modified,
        ref_unbind_lmt,
        bind=[True, None, False, None, True],
        ts=[1, None, None, None, None],
    )


def _scenario_tsd_rebind_delta_bridge() -> dict[str, Any]:
    @compute_node
    def get_delta(tsd: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return tsd.delta_value if tsd.modified else None

    @graph
    def rebind_bridge(select_left: TS[bool], left: TSD[str, TS[int]], right: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return get_delta(switch_(select_left, {True: lambda l, r: l, False: lambda l, r: r}, left, right))

    @graph
    def rebind_bridge_valid(select_left: TS[bool], left: TSD[str, TS[int]], right: TSD[str, TS[int]]) -> TS[bool]:
        return valid(rebind_bridge(select_left, left, right))

    @graph
    def rebind_bridge_modified(select_left: TS[bool], left: TSD[str, TS[int]], right: TSD[str, TS[int]]) -> TS[bool]:
        return modified(rebind_bridge(select_left, left, right))

    @graph
    def rebind_bridge_lmt(select_left: TS[bool], left: TSD[str, TS[int]], right: TSD[str, TS[int]]) -> TS[datetime]:
        return last_modified_time(rebind_bridge(select_left, left, right))

    return _collect_ts_trace(
        rebind_bridge,
        rebind_bridge_valid,
        rebind_bridge_modified,
        rebind_bridge_lmt,
        select_left=[True, None, False, True, False, True],
        left=[{"a": 1, "b": 2}, {"b": 3}, None, None, None, None],
        right=[{"x": 10}, None, {"y": 20}, None, None, None],
    )


@compute_node
def _select_tss_ref(index: TS[int], ts: TSL[REF[TSS[int]], Size[2]]) -> REF[TSS[int]]:
    from typing import cast

    return cast(REF[TSS[int]], ts[index.value].value)


@compute_node
def _select_tsd_ref(index: TS[int], ts: TSL[REF[TSD[int, TS[int]]], Size[2]]) -> REF[TSD[int, TS[int]]]:
    from typing import cast

    return cast(REF[TSD[int, TS[int]]], ts[index.value].value)


def _scenario_ref_toggle_set_and_dict() -> dict[str, Any]:
    @graph
    def toggle_set(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> REF[TSS[int]]:
        return _select_tss_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def toggle_set_valid(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[bool]:
        return valid(toggle_set(index, ts1, ts2))

    @graph
    def toggle_set_modified(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[bool]:
        return modified(toggle_set(index, ts1, ts2))

    @graph
    def toggle_set_lmt(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[datetime]:
        return last_modified_time(toggle_set(index, ts1, ts2))

    @graph
    def toggle_dict(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> REF[TSD[int, TS[int]]]:
        return _select_tsd_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def toggle_dict_valid(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[bool]:
        return valid(toggle_dict(index, ts1, ts2))

    @graph
    def toggle_dict_modified(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[bool]:
        return modified(toggle_dict(index, ts1, ts2))

    @graph
    def toggle_dict_lmt(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[datetime]:
        return last_modified_time(toggle_dict(index, ts1, ts2))

    return {
        "set": _collect_ts_trace(
            toggle_set,
            toggle_set_valid,
            toggle_set_modified,
            toggle_set_lmt,
            index=[0, 1, 0, 1, 0],
            ts1=[{1, 2}, None, None, None, None],
            ts2=[{10, 20}, None, None, None, None],
        ),
        "dict": _collect_ts_trace(
            toggle_dict,
            toggle_dict_valid,
            toggle_dict_modified,
            toggle_dict_lmt,
            index=[0, 1, 0, 1, 0],
            ts1=[{1: 1, 2: 2}, None, None, None, None],
            ts2=[{10: 10, 20: 20}, None, None, None, None],
        ),
    }


def _scenario_ref_merge_delta_semantics() -> dict[str, Any]:
    @graph
    def merge_set(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> REF[TSS[int]]:
        return _select_tss_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def merge_set_valid(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[bool]:
        return valid(merge_set(index, ts1, ts2))

    @graph
    def merge_set_modified(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[bool]:
        return modified(merge_set(index, ts1, ts2))

    @graph
    def merge_set_lmt(index: TS[int], ts1: TSS[int], ts2: TSS[int]) -> TS[datetime]:
        return last_modified_time(merge_set(index, ts1, ts2))

    @graph
    def merge_dict(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> REF[TSD[int, TS[int]]]:
        return _select_tsd_ref(index, TSL.from_ts(ts1, ts2))

    @graph
    def merge_dict_valid(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[bool]:
        return valid(merge_dict(index, ts1, ts2))

    @graph
    def merge_dict_modified(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[bool]:
        return modified(merge_dict(index, ts1, ts2))

    @graph
    def merge_dict_lmt(index: TS[int], ts1: TSD[int, TS[int]], ts2: TSD[int, TS[int]]) -> TS[datetime]:
        return last_modified_time(merge_dict(index, ts1, ts2))

    return {
        "set": _collect_ts_trace(
            merge_set,
            merge_set_valid,
            merge_set_modified,
            merge_set_lmt,
            index=[0, None, 1, None],
            ts1=[{1, 2}, None, None, {4}],
            ts2=[{-1}, {-2}, {-3, Removed(-1)}, {-4}],
        ),
        "dict": _collect_ts_trace(
            merge_dict,
            merge_dict_valid,
            merge_dict_modified,
            merge_dict_lmt,
            index=[0, None, 1, None],
            ts1=[{1: 1, 2: 2}, None, None, {4: 4}],
            ts2=[{-1: -1}, {-2: -2}, {-3: -3, -1: REMOVE}, {-4: -4}],
        ),
    }


def _scenario_mesh_nested_binding() -> dict[str, Any]:
    @graph
    def get_arg(name: TS[str], vars: TSD[str, TS[float]]) -> TS[float]:
        return switch_(
            contains_(vars, name),
            {True: lambda n, v: v[n], False: lambda n, v: mesh_(operation)[n]},
            n=name,
            v=vars,
        )

    @graph
    def perform_op(op_name: TS[str], lhs: TS[float], rhs: TS[float]) -> TS[float]:
        return switch_(
            op_name,
            {"+": lambda l, r: l + r, "-": lambda l, r: l - r, "*": lambda l, r: l * r, "/": lambda l, r: l / r},
            lhs,
            rhs,
        )

    @graph
    def operation(i: TS[tuple[str, str, str]], vars: TSD[str, TS[float]]) -> TS[float]:
        return perform_op(i[0], get_arg(i[1], vars), get_arg(i[2], vars))

    @graph
    def nested_mesh(i: TSD[str, TS[tuple[str, str, str]]], vars: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return mesh_(operation, i, pass_through(vars))

    @graph
    def nested_mesh_valid(i: TSD[str, TS[tuple[str, str, str]]], vars: TSD[str, TS[float]]) -> TS[bool]:
        return valid(nested_mesh(i, vars))

    @graph
    def nested_mesh_modified(i: TSD[str, TS[tuple[str, str, str]]], vars: TSD[str, TS[float]]) -> TS[bool]:
        return modified(nested_mesh(i, vars))

    @graph
    def nested_mesh_lmt(i: TSD[str, TS[tuple[str, str, str]]], vars: TSD[str, TS[float]]) -> TS[datetime]:
        return last_modified_time(nested_mesh(i, vars))

    return _collect_ts_trace(
        nested_mesh,
        nested_mesh_valid,
        nested_mesh_modified,
        nested_mesh_lmt,
        i=[{"c": ("+", "a", "b"), "d": ("-", "c", "x")}, {"e": ("*", "d", "a")}],
        vars=[{"a": 1.0, "b": 2.0, "x": 3.0}, None, None, {"a": 2.0}],
    )


def _scenario_mesh_contains_recursive() -> dict[str, Any]:
    @graph
    def mesh_contains_prev(key: TS[int]) -> TS[bool]:
        return contains_(mesh_("_"), key - 1)

    @graph
    def mesh_contains(keys: TSS[int]) -> TSD[int, TS[bool]]:
        return mesh_(mesh_contains_prev, __keys__=keys, __name__="_")

    @graph
    def mesh_contains_valid(keys: TSS[int]) -> TS[bool]:
        return valid(mesh_contains(keys))

    @graph
    def mesh_contains_modified(keys: TSS[int]) -> TS[bool]:
        return modified(mesh_contains(keys))

    @graph
    def mesh_contains_lmt(keys: TSS[int]) -> TS[datetime]:
        return last_modified_time(mesh_contains(keys))

    return _collect_ts_trace(
        mesh_contains,
        mesh_contains_valid,
        mesh_contains_modified,
        mesh_contains_lmt,
        keys=[{1}, {2}, {3}, {5}, None, {4}],
    )


def _scenario_map_reference_cleanup() -> dict[str, Any]:
    @graph
    def map_cleanup(value: TSD[str, TS[int]]) -> TSD[str, TS[str]]:
        m1 = map_(lambda key, v: format_("{}_{}_1", key, v), value)
        return map_(lambda key, v: format_("{}_2", v), m1)

    @graph
    def map_cleanup_valid(value: TSD[str, TS[int]]) -> TS[bool]:
        return valid(map_cleanup(value))

    @graph
    def map_cleanup_modified(value: TSD[str, TS[int]]) -> TS[bool]:
        return modified(map_cleanup(value))

    @graph
    def map_cleanup_lmt(value: TSD[str, TS[int]]) -> TS[datetime]:
        return last_modified_time(map_cleanup(value))

    return _collect_ts_trace(
        map_cleanup,
        map_cleanup_valid,
        map_cleanup_modified,
        map_cleanup_lmt,
        value=[{"a": 1, "b": 2}, {"b": REMOVE}, {"a": 2}],
    )


def _scenario_map_input_goes_away() -> dict[str, Any]:
    @compute_node
    def check(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return ts.delta_value

    @graph
    def check_g(ts: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return switch_(
            count(ts) > 1,
            {
                False: lambda i: i,
                True: lambda i: check(i),
            },
            ts,
        )

    @graph
    def map_goes_away(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TSD[str, TS[int]]:
        return check_g(map_(lambda x, y: x + y, tsd1, tsd2))

    @graph
    def map_goes_away_valid(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TS[bool]:
        return valid(map_goes_away(tsd1, tsd2))

    @graph
    def map_goes_away_modified(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TS[bool]:
        return modified(map_goes_away(tsd1, tsd2))

    @graph
    def map_goes_away_lmt(tsd1: TSD[str, TS[int]], tsd2: TSD[str, TS[int]]) -> TS[datetime]:
        return last_modified_time(map_goes_away(tsd1, tsd2))

    return _collect_ts_trace(
        map_goes_away,
        map_goes_away_valid,
        map_goes_away_modified,
        map_goes_away_lmt,
        tsd1=[{"a": 1, "b": 2, "c": 0}, None, {"a": 3, "c": REMOVE}],
        tsd2=[{"a": 10, "b": 20}, {"a": 11, "b": REMOVE}, {"a": REMOVE, "b": 21}],
    )


def _scenario_nested_switch_map() -> dict[str, Any]:
    @generator
    def _switch_generator(key: str, _clock: EvaluationClock = None) -> TS[str]:
        for i in range(5):
            yield _clock.next_cycle_evaluation_time, f"{key}_{i}"

    @graph
    def one_() -> TS[str]:
        return _switch_generator("one")

    @graph
    def two_() -> TS[str]:
        return _switch_generator("two")

    @graph
    def _switch(key: TS[str]) -> TS[str]:
        key = default(const("two", delay=MIN_TD * 3), key)
        return switch_(key, {"one": one_, "two": two_})

    @graph
    def nested_switch_map(keys: TSS[str]) -> TSD[str, TS[str]]:
        return map_(_switch, __keys__=keys, __key_arg__="key")

    @graph
    def nested_switch_map_valid(keys: TSS[str]) -> TS[bool]:
        return valid(nested_switch_map(keys))

    @graph
    def nested_switch_map_modified(keys: TSS[str]) -> TS[bool]:
        return modified(nested_switch_map(keys))

    @graph
    def nested_switch_map_lmt(keys: TSS[str]) -> TS[datetime]:
        return last_modified_time(nested_switch_map(keys))

    return _collect_ts_trace(
        nested_switch_map,
        nested_switch_map_valid,
        nested_switch_map_modified,
        nested_switch_map_lmt,
        keys=[{"one"}, None, {"two"}],
    )


def _scenario_map_input_rebind_to_nonpeer() -> dict[str, Any]:
    @graph
    def map_rebind(ts: TS[int]) -> TSD[str, TS[int]]:
        initial = const({"a": 1, "b": 2}, TSD[str, TS[int]])
        source = switch_(
            ts > 0,
            {
                False: lambda i, replace_refs: i,
                True: lambda i, replace_refs: map_(
                    lambda x, replace_refs_: if_then_else(replace_refs_, x + 1, x), i, replace_refs
                ),
            },
            initial,
            ts > 1,
        )
        return map_(lambda x, y: x + y, source, ts)

    @graph
    def map_rebind_valid(ts: TS[int]) -> TS[bool]:
        return valid(map_rebind(ts))

    @graph
    def map_rebind_modified(ts: TS[int]) -> TS[bool]:
        return modified(map_rebind(ts))

    @graph
    def map_rebind_lmt(ts: TS[int]) -> TS[datetime]:
        return last_modified_time(map_rebind(ts))

    return _collect_ts_trace(
        map_rebind,
        map_rebind_valid,
        map_rebind_modified,
        map_rebind_lmt,
        ts=[0, 1, 2, 3, 4],
    )


def _scenario_tsd_remove_and_unbind_same_cycle() -> dict[str, Any]:
    @graph
    def remove_and_unbind(tsd: TSD[str, TSD[int, TS[int]]], gate: TS[bool]) -> TSD[str, TSD[int, TS[int]]]:
        tsd1 = if_(gate, tsd).true
        return filter_(True, tsd1)

    @graph
    def remove_and_unbind_valid(tsd: TSD[str, TSD[int, TS[int]]], gate: TS[bool]) -> TS[bool]:
        return valid(remove_and_unbind(tsd, gate))

    @graph
    def remove_and_unbind_modified(tsd: TSD[str, TSD[int, TS[int]]], gate: TS[bool]) -> TS[bool]:
        return modified(remove_and_unbind(tsd, gate))

    @graph
    def remove_and_unbind_lmt(tsd: TSD[str, TSD[int, TS[int]]], gate: TS[bool]) -> TS[datetime]:
        return last_modified_time(remove_and_unbind(tsd, gate))

    return _collect_ts_trace(
        remove_and_unbind,
        remove_and_unbind_valid,
        remove_and_unbind_modified,
        remove_and_unbind_lmt,
        tsd=[
            {"a": {1: 1}, "b": {2: 2}},
            None,
            {"a": {1: 2}, "b": REMOVE},
            None,
            {"a": {1: 3}, "b": {3: 3}},
        ],
        gate=[True, None, False, None, True],
    )


def _scenario_tsd_in_bundle_ref() -> dict[str, Any]:
    @graph
    def source(i: TSS[int], v: TS[int]) -> TSB[_NestedBundleSchema]:
        m = map_(lambda key, x: map_(lambda y: y, key, __keys__=x), pass_through(i), __keys__=i)
        return switch_(
            v,
            {
                0: lambda i, m: combine[TSB[_NestedBundleSchema]](a=0, b=m),
                1: lambda i, m: combine[TSB[_NestedBundleSchema]](a=1, b=m),
            },
            i,
            m,
        )

    @graph
    def source_valid(i: TSS[int], v: TS[int]) -> TS[bool]:
        return valid(source(i, v))

    @graph
    def source_modified(i: TSS[int], v: TS[int]) -> TS[bool]:
        return modified(source(i, v))

    @graph
    def source_lmt(i: TSS[int], v: TS[int]) -> TS[datetime]:
        return last_modified_time(source(i, v))

    return _collect_ts_trace(
        source,
        source_valid,
        source_modified,
        source_lmt,
        i=[{0}, None, {1}, {Removed(0)}, {0, 1}],
        v=[0, 1, None, None, 1],
    )


def _scenario_ref_race_tsd_bundle_switch() -> dict[str, Any]:
    @compute_node
    def make_ref(ts: TS[int], ref: REF[TS[int]]) -> REF[TS[int]]:
        return ref.value if ts.value != 0 else TimeSeriesReference.make()

    @graph
    def make_bundle(ts: TSB[_RaceBundleSwitchSchema]) -> TSB[_RaceBundleSchema]:
        return switch_(
            ts.free,
            {
                False: lambda a, b, cond: filter_(cond, combine[TSB[_RaceBundleSchema]](a=a, b=b)),
                True: lambda a, b, cond: combine[TSB[_RaceBundleSchema]](a=make_ref(a, a), b=make_ref(b, b)),
            },
            reload_on_ticked=True,
            a=ts.a,
            b=ts.b,
            cond=ts.cond,
        )

    @graph
    def bundle_race(ts: TSD[int, TSB[_RaceBundleSwitchSchema]]) -> REF[TSB[_RaceBundleSchema]]:
        refs = map_(make_bundle, ts)
        return reduce_tsd_of_bundles_with_race(tsd=refs)

    @graph
    def bundle_race_valid(ts: TSD[int, TSB[_RaceBundleSwitchSchema]]) -> TS[bool]:
        return valid(bundle_race(ts))

    @graph
    def bundle_race_modified(ts: TSD[int, TSB[_RaceBundleSwitchSchema]]) -> TS[bool]:
        return modified(bundle_race(ts))

    @graph
    def bundle_race_lmt(ts: TSD[int, TSB[_RaceBundleSwitchSchema]]) -> TS[datetime]:
        return last_modified_time(bundle_race(ts))

    return _collect_ts_trace(
        bundle_race,
        bundle_race_valid,
        bundle_race_modified,
        bundle_race_lmt,
        ts=[
            {1: {"free": False}, 2: {"free": True}},
            {1: {"a": 0, "cond": False}},
            {1: {"a": 0, "cond": True}},
            {2: {"a": 2, "b": 1}},
            {1: {"a": 1, "b": 2}},
            {1: {"free": False, "cond": False}},
            {1: {"a": 3, "b": 3, "cond": True}},
            {2: REMOVE},
            {2: {"a": 0, "b": 0}},
        ],
    )


SCENARIOS = {
    "map_input_goes_away": _scenario_map_input_goes_away,
    "map_reference_cleanup": _scenario_map_reference_cleanup,
    "map_input_rebind_to_nonpeer": _scenario_map_input_rebind_to_nonpeer,
    "mesh_contains_recursive": _scenario_mesh_contains_recursive,
    "mesh_nested_binding": _scenario_mesh_nested_binding,
    "nested_map_switch_reduce": _scenario_nested_map_switch_reduce,
    "nested_switch_map": _scenario_nested_switch_map,
    "ref_merge_from_tsl_input": _scenario_ref_merge_from_tsl_input,
    "ref_merge_delta_semantics": _scenario_ref_merge_delta_semantics,
    "ref_race_tsd_to_ts": _scenario_ref_race_tsd_to_ts,
    "ref_race_tsd_bundle_switch": _scenario_ref_race_tsd_bundle_switch,
    "ref_toggle_set_and_dict": _scenario_ref_toggle_set_and_dict,
    "ref_tsl_inner_switch": _scenario_ref_tsl_inner_switch,
    "ref_toggle_to_ts": _scenario_ref_toggle_to_ts,
    "ref_unbind_active_transition": _scenario_ref_unbind_active_transition,
    "tsd_in_bundle_ref": _scenario_tsd_in_bundle_ref,
    "tsd_remove_and_unbind_same_cycle": _scenario_tsd_remove_and_unbind_same_cycle,
    "tsd_rebind_delta_bridge": _scenario_tsd_rebind_delta_bridge,
}


def run_trace_scenario(name: str) -> dict[str, Any]:
    if name not in SCENARIOS:
        raise KeyError(f"Unknown scenario '{name}'")
    return SCENARIOS[name]()
