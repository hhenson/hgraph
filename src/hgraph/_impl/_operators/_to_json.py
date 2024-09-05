from datetime import datetime, date, time, timedelta
from enum import Enum
from functools import singledispatch, cache
from typing import Callable, Any

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
)

__all__ = []


@cache
def _to_json(tp: TIME_SERIES_TYPE) -> Any:
    return to_json_converter(HgTypeMetaData.parse_type(tp))


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


@compute_node(overloads=to_json)
def to_json_generic(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[str]:
    return _to_json(_tp)(ts)
