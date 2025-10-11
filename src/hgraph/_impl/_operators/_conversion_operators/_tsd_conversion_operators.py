from collections import deque
from typing import Tuple, Type, Mapping, Set

from hgraph import (
    REMOVE_IF_EXISTS,
    compute_node,
    OUT,
    TS,
    combine,
    TSL,
    SCALAR,
    SIZE,
    TIME_SERIES_TYPE,
    TSD,
    REF,
    convert,
    KEYABLE_SCALAR,
    DEFAULT,
    TSD_OUT,
    REMOVE,
    collect,
    SIGNAL,
    TSS,
    emit,
    V,
    STATE,
    SCHEDULER,
    TSB,
    MIN_TD,
    SCALAR_1,
    values_,
    set_delta,
    AUTO_RESOLVE,
)
from hgraph._impl._operators._conversion_operators._conversion_operator_util import _BufferState, KeyValue

__all__ = []


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[TIME_SERIES_TYPE].py_type]),
)
def convert_ts_to_tsd(
    key: TS[KEYABLE_SCALAR],
    ts: REF[TIME_SERIES_TYPE],
    to: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, REF[TIME_SERIES_TYPE]] = None,
) -> TSD[KEYABLE_SCALAR, REF[TIME_SERIES_TYPE]]:
    remove = {k: REMOVE for k in _output.keys() if k != key.value} if _output.valid and key.modified else {}
    return {key.value: ts.value, **remove}


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD or m[OUT].matches_type(TSD[int, TS[m[SCALAR].py_type]]),
)
def convert_tuple_to_enumerated_tsd(
    ts: TS[Tuple[SCALAR, ...]], _output: TSD_OUT[int, TS[SCALAR]] = None
) -> TSD[int, TS[SCALAR]]:
    ts = ts.value
    out = {}
    for i, v in enumerate(ts):
        ov = _output.get(i)
        if ov is None or ov.value != v:
            out[i] = v
    if len(_output) > len(ts):
        for i in range(len(ts), len(_output)):
            out[i] = REMOVE
    return out


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[TIME_SERIES_TYPE].py_type]),
)
def convert_set_to_tsd(
    keys: TS[Set[KEYABLE_SCALAR]],
    ts: TIME_SERIES_TYPE,
    to: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in _output.keys() if k not in keys.value} if _output.valid and keys.modified else {}
    if ts.modified:
        update = {k: ts.value for k in keys.value}
    else:
        update = {k: ts.value for k in keys.value if k not in _output.keys()}
    return remove | update


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[TIME_SERIES_TYPE].py_type]),
)
def convert_tss_to_tsd(
    keys: TSS[KEYABLE_SCALAR], ts: TIME_SERIES_TYPE, to: Type[OUT] = DEFAULT[OUT]
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in keys.removed()}
    if ts.modified:
        update = {k: ts.value for k in keys.value}
    else:
        update = {k: ts.value for k in keys.added()}

    return remove | update


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD or m[OUT].matches_type(TSD[int, m[TIME_SERIES_TYPE].py_type]),
)
def convert_tsl_to_tsd(
    ts: TSL[REF[TIME_SERIES_TYPE], SIZE], to: Type[OUT] = DEFAULT[OUT]
) -> TSD[int, REF[TIME_SERIES_TYPE]]:
    return {k: i.value for k, i in enumerate(ts) if i.valid}


@compute_node(
    overloads=convert,
    requires=lambda m, s: m[OUT].py_type is TSD
                          or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, TS[m[SCALAR].py_type]]),
    resolvers={TIME_SERIES_TYPE: lambda m, s: TS[m[SCALAR]] if m[OUT].py_type is TSD else m[OUT].value_tp},
)
def convert_mapping_to_tsd(
    ts: TS[Mapping[KEYABLE_SCALAR, SCALAR]],
    to: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in _output.keys() if k not in ts.value.keys()}
    return ts.value | remove


@compute_node(
    overloads=combine,
    requires=lambda m, s: (
        (m[OUT].py_type == TSD or m[OUT].matches_type(TSD[m[SCALAR], m[TIME_SERIES_TYPE]]))
        and (
            len(s["keys"]) == m[SIZE].py_type.SIZE
            or f"Length of keys ({len(s['keys'])}) and values ({m[SIZE].py_type.SIZE}) does not match"
        )
    ),
    all_valid=lambda m, s: ("tsl",) if s["__strict__"] else None,
)
def combine_tsd_from_tuple_and_tsl(
    keys: Tuple[SCALAR, ...], *tsl: TSL[REF[TIME_SERIES_TYPE], SIZE], __strict__: bool = True
) -> TSD[SCALAR, REF[TIME_SERIES_TYPE]]:
    # Not the valid constraint only affects wheather or not this has a binding and not if the underlying value is valid!
    return {k: v.value for k, v in zip(keys, tsl) if v.valid and not v.value.is_empty}


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type == TSD or m[OUT].matches_type(TSD[m[SCALAR], m[TIME_SERIES_TYPE]]),
    all_valid=lambda m, s: (
        (
            "keys",
            "tsl",
        )
        if s["__strict__"]
        else None
    ),
)
def combine_tsd_from_tsl_and_tsl(
    keys: TSL[TS[SCALAR], SIZE],
    tsl: TSL[REF[TIME_SERIES_TYPE], SIZE],
    __strict__: bool = True,
    _output: TSD_OUT[SCALAR, REF[TIME_SERIES_TYPE]] = None,
) -> TSD[SCALAR, REF[TIME_SERIES_TYPE]]:
    out = {k.value: v.value for k, v in zip(keys, tsl) if k.valid}
    if _output.valid:
        out |= {k: REMOVE for k in _output if k not in out}
    return out


@compute_node(
    overloads=combine,
    requires=lambda m, s: m[OUT].py_type == TSD or m[OUT].matches_type(TSD[m[SCALAR], TS[m[SCALAR_1]]]),
)
def combine_tsd_from_tuple_and_tuple(
    keys: TS[tuple[SCALAR, ...]],
    values: TS[tuple[SCALAR_1, ...]],
    __strict__: bool = True,
    _output: TSD_OUT[SCALAR, TS[SCALAR_1]] = None,
) -> TSD[SCALAR, TS[SCALAR_1]]:
    keys_ = keys.value
    values_ = values.value
    out_value = _output.value if _output.valid else {}
    remove = {k: REMOVE for k in out_value if k not in keys_} if _output.valid and keys.modified else {}
    add = {k: v for k, v in zip(keys_, values_) if k not in out_value or out_value[k] != v}
    return add | remove


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[TIME_SERIES_TYPE].py_type]),
    valid=("key", "ts"),
)
def collect_tsd(
    key: TS[KEYABLE_SCALAR],
    ts: TIME_SERIES_TYPE,
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in _output.keys()} if reset.modified else {}
    new = {key.value: ts.value} if ts.modified else {}
    return remove | new


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[SCALAR].py_type]),
    resolvers={TIME_SERIES_TYPE: lambda m, s: TS[m[SCALAR]] if m[OUT].py_type is TSD else m[OUT].value_tp},
    valid=("keys", "ts"),
)
def collect_tsd_from_tuples(
    keys: TS[Tuple[KEYABLE_SCALAR, ...]],
    ts: TS[Tuple[SCALAR, ...]],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in _output.keys()} if reset.modified else {}
    new = {k: v for k, v in zip(keys.value, ts.value)} if keys.modified or ts.modified else {}
    return remove | new


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[TIME_SERIES_TYPE].py_type]),
    valid=("tsd",),
)
def collect_tsd_from_tsd(
    tsd: TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE],
    *,
    reset: SIGNAL = None,
    exclude: TSS[KEYABLE_SCALAR] = None,
    tp_: Type[OUT] = TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = (
        {k: REMOVE for k in _output.keys()}
        if reset.modified
        else {k: REMOVE_IF_EXISTS for k in exclude.added()} if exclude.modified else {}
    )
    exclusions = exclude.value if exclude.valid else set()
    return remove | {k: v.value for k, v in tsd.modified_items() if k not in exclusions}


@compute_node(
    overloads=collect,
    requires=lambda m, s: m[OUT].py_type is TSD
    or m[OUT].matches_type(TSD[m[KEYABLE_SCALAR].py_type, m[SCALAR].py_type]),
    resolvers={TIME_SERIES_TYPE: lambda m, s: TS[m[SCALAR]] if m[OUT].py_type is TSD else m[OUT].value_tp},
    valid=("ts",),
)
def collect_tsd_from_mappings(
    ts: TS[Mapping[KEYABLE_SCALAR, SCALAR]],
    *,
    reset: SIGNAL = None,
    tp_: Type[OUT] = DEFAULT[OUT],
    _output: TSD_OUT[KEYABLE_SCALAR, TIME_SERIES_TYPE] = None,
) -> TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE]:
    remove = {k: REMOVE for k in _output.keys()} if reset.modified else {}
    new = {k: v for k, v in ts.value.items()} if ts.modified else {}
    return remove | new


@compute_node(overloads=emit, resolvers={OUT: lambda m, s: TS[m[TIME_SERIES_TYPE].scalar_type()]})
def emit_tsd(
    ts: TSD[KEYABLE_SCALAR, TIME_SERIES_TYPE],
    v_: Type[V] = DEFAULT[OUT],
    _state: STATE[_BufferState] = None,
    _schedule: SCHEDULER = None,
) -> TSB[KeyValue[KEYABLE_SCALAR, V]]:
    """
    Converts a tuple of KeyValue values in a stream of individual SCALAR values.
    """
    if ts.modified:
        _state.buffer.extend([(k, v.value) for k, v in ts.modified_items()])

    if _state.buffer:
        d: deque[SCALAR] = _state.buffer
        k, v = d.popleft()
        if d:
            _schedule.schedule(MIN_TD)
        return {"key": k, "value": v}


@compute_node(
    overloads=values_,
)
def values_tsd_to_tss(
    ts: TSD[SCALAR, TS[SCALAR_1]], _output: TSS[SCALAR_1] = None, _tp: type[SCALAR] = AUTO_RESOLVE
) -> TSS[SCALAR_1]:
    """This is likely to be a rather expensive operation on a large tsd, so use carefully"""
    if not _output.valid:
        return {v.value for v in ts.values()}
    else:
        current = _output.value
        all = {v.value for v in ts.values()}
        added = {v for v in all if v not in current}
        removed = {v for v in current if v not in all}
        return set_delta(added=added, removed=removed, tp=_tp)
