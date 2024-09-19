import json
from datetime import datetime, date, time, timedelta
from enum import Enum
from functools import singledispatch
from typing import Callable, Any

from frozendict import frozendict as fd

from hgraph import (
    compute_node,
    to_json,
    TS,
    HgTypeMetaData,
    HgCompoundScalarType,
    HgAtomicType,
    HgTSTypeMetaData,
    TIME_SERIES_TYPE,
    AUTO_RESOLVE,
    HgDictScalarType,
    HgTupleCollectionScalarType,
    from_json,
    OUT,
    DEFAULT,
)

__all__ = []

from hgraph._operators._to_json import to_json_builder, from_json_builder


@singledispatch
def to_json_converter(value: HgTypeMetaData) -> Callable[[Any], str]:
    raise RuntimeError(f"Cannot convert {value} to JSON")


@to_json_converter.register(HgCompoundScalarType)
def _(value: HgCompoundScalarType) -> Callable[[Any], str]:
    to_json = []
    for k, tp in value.meta_data_schema.items():
        m = to_json_converter(tp)
        to_json.append(lambda v, m=m, k=k: f'"{k}": {m(v_)}' if (v_ := getattr(v, k, None)) is not None else "")
    return lambda v, to_json=to_json: f'{{ {", ".join(v_ for fn in to_json if (v_:=fn(v)))} }}'


@to_json_converter.register(HgAtomicType)
def _(value: HgAtomicType) -> Callable[[Any], str]:
    if issubclass(value.py_type, Enum):
        return lambda v: f'"{v.name}"'

    return {
        bool: lambda v: None if v is None else "true" if v else "false",
        int: lambda v: None if v is None else f"{v}",
        float: lambda v: None if v is None else f"{v}",
        str: lambda v: None if v is None else f'"{v}"',
        date: lambda v: None if v is None else f'"{v.strftime("%Y-%m-%d")}"',
        time: lambda v: None if v is None else f'"{v.strftime("%H:%M:%S.%f")}"',
        timedelta: _td_to_str,
        datetime: lambda v: None if v is None else f'"{v.strftime("%Y-%m-%d %H:%M:%S.%f")}"',
    }[value.py_type]


@to_json_converter.register(HgDictScalarType)
def _(value: HgDictScalarType) -> Callable[[Any], str]:
    k_fn = to_json_converter(value.key_type)
    v_fn = to_json_converter(value.value_type)

    def _to_json(v, k_fn=k_fn, v_fn=v_fn):
        items = (f"{k_fn(k)}: {v_fn(v_)}" for k, v_ in v.items())
        return f'{{ {", ".join(items)} }}'

    return _to_json


@to_json_converter.register(HgTupleCollectionScalarType)
def _(value: HgTupleCollectionScalarType) -> Callable[[Any], str]:
    v_fn = to_json_converter(value.element_type)
    return lambda v, v_fn=v_fn: f'[ {", ".join(v_fn(i) for i in v)} ]'


@to_json_converter.register(HgTSTypeMetaData)
def _(value: HgTSTypeMetaData) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp)
    return lambda v, fn=fn: fn(v.value) if v.valid else ""


def _td_to_str(delta: timedelta) -> str:
    if delta is None:
        return None
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    ms = delta.microseconds
    return f'"{days}:{hours}:{minutes}:{seconds}.{ms:06}"'


@singledispatch
def from_json_converter(value: HgTypeMetaData) -> Callable[[dict], Any]:
    """By default, just assume the value can be returned as is"""
    raise RuntimeError(f"Cannot convert to '{value}' from JSON")


@from_json_converter.register(HgTSTypeMetaData)
def _(value: HgTSTypeMetaData) -> Callable[[Any], Any]:
    return from_json_converter(value.value_scalar_tp)


@from_json_converter.register(HgCompoundScalarType)
def _(value: HgCompoundScalarType) -> Callable[[Any], Any]:
    fns = []
    for k, tp in value.meta_data_schema.items():
        fns.append((k, lambda v, tp=tp, k=k: from_json_builder(tp)(v.get(k, None))))
    return lambda v, fns=fns, tp=value.py_type: tp(**{k: v_ for k, fn in fns if (v_ := fn(v)) is not None})


@from_json_converter.register(HgAtomicType)
def _(value: HgAtomicType) -> Callable[[Any], Any]:
    if issubclass(value.py_type, Enum):
        return lambda v, tp=value.py_type: None if v is None else getattr(tp, v)

    return {
        date: lambda v: None if v is None else datetime.strptime(v, "%Y-%m-%d").date(),
        time: lambda v: None if v is None else datetime.strptime(v, "%H:%M:%S.%f").time(),
        timedelta: _str_to_td,
        datetime: lambda v: None if v is None else datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f"),
    }.get(value.py_type, lambda v: v)


def _str_to_td(s: str) -> timedelta:
    if s is None:
        return None
    days, hours, minutes, seconds_ms = s.split(":")
    seconds, ms = seconds_ms.split(".")
    return timedelta(
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(seconds),
        microseconds=int(ms),
    )


@from_json_converter.register(HgDictScalarType)
def _(value: HgDictScalarType) -> Callable[[dict], Any]:
    k_fn = from_json_converter(value.key_type)
    v_fn = from_json_converter(value.value_type)
    return lambda v, k_fn=k_fn, v_fn=v_fn: fd(**{k_fn(k_): v_fn(v_) for k_, v_ in v.items()})


@from_json_converter.register(HgTupleCollectionScalarType)
def _(value: HgTupleCollectionScalarType) -> Callable[[list], Any]:
    v_fn = from_json_converter(value.element_type)
    return lambda v, v_fn=v_fn: tuple(v_fn(v_fn(i)) for i in v)


@compute_node(overloads=to_json)
def to_json_generic(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[str]:
    return to_json_builder(_tp)(ts)


@compute_node(overloads=from_json)
def from_json_generic(ts: TS[str], _tp: type[OUT] = AUTO_RESOLVE) -> DEFAULT[OUT]:
    value = json.loads(ts.value)
    return from_json_builder(_tp)(value)
