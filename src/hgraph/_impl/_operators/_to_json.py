import json
from datetime import datetime, date, time, timedelta
from enum import Enum
from functools import singledispatch
from itertools import chain
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
    HgTSLTypeMetaData,
    HgTSSTypeMetaData,
    HgTSBTypeMetaData,
    HgTSDTypeMetaData, REMOVE_IF_EXISTS,
)

__all__ = []

from hgraph._operators._to_json import to_json_builder, from_json_builder


@singledispatch
def to_json_converter(value: HgTypeMetaData, delta=False) -> Callable[[Any], str]:
    raise RuntimeError(f"Cannot convert {value} to JSON")

@to_json_converter.register(HgCompoundScalarType)
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], str]:
    to_json = []
    for k, tp in value.meta_data_schema.items():
        m = to_json_converter(tp, delta)
        to_json.append(lambda v, m=m, k=k: f'"{k}": {m(v_)}' if (v_ := getattr(v, k, None)) is not None else "")
    return lambda v, to_json=to_json: f'{{{", ".join(v_ for fn in to_json if (v_:=fn(v)))}}}'

@to_json_converter.register(HgAtomicType)
def _(value: HgAtomicType, delta=False) -> Callable[[Any], str]:
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
def _(value: HgDictScalarType, delta=False) -> Callable[[Any], str]:
    k_fn = to_json_converter(value.key_type, delta)
    if not issubclass(value.key_type.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn=k_fn_inner: json.dumps(k_fn(k))  # escape the string
    v_fn = to_json_converter(value.value_type, delta)

    def _to_json(v, k_fn=k_fn, v_fn=v_fn):
        items = (f"{k_fn(k)}: {v_fn(v_)}" for k, v_ in v.items())
        return f'{{{", ".join(items)}}}'

    return _to_json


@to_json_converter.register(HgTupleCollectionScalarType)
def _(value: HgTupleCollectionScalarType, delta=False) -> Callable[[Any], str]:
    v_fn = to_json_converter(value.element_type, delta)
    return lambda v, v_fn=v_fn: f'[{", ".join(v_fn(i) for i in v)}]'


@to_json_converter.register(HgTSTypeMetaData)
def _(value: HgTSTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp, delta)
    return lambda v, fn=fn: fn(v.value) if v.valid else ""


@to_json_converter.register(HgTSLTypeMetaData)
def _(value: HgTSLTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_tp, delta)

    def qstr(i):
        return f'"{str(i)}"'

    if delta:
        return lambda v, fn=fn: f'{{{", ".join(qstr(i) + ": " + fn(t) for i, t in v.modified_items())}}}'
    else:
        return lambda v, fn=fn: f'[{", ".join(fn(i) for i in v)}]'


@to_json_converter.register(HgTSBTypeMetaData)
def _(value: HgTSBTypeMetaData, delta=False) -> Callable[[Any], str]:
    schema = {}
    for k, tp in value.bundle_schema_tp.meta_data_schema.items():
        f = to_json_converter(tp, delta)
        schema[k] = lambda i, t, f=f: f'"{i}": {f(t)}'
    if delta:
        return lambda v, schema=schema: f'{{{", ".join(schema[i](i, t) for i, t in v.items())}}}'
    else:
        return lambda v, schema=schema: f'{{{", ".join(schema[i](i, t) for i, t in v.modified_items())}}}'


@to_json_converter.register(HgTSSTypeMetaData)
def _(value: HgTSSTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp, delta)
    if not delta:
        return lambda v, fn=fn: f'[{", ".join(fn(i) for i in v.values())}]'
    else:
        def f_i(a, v, f):
            return f'"{a}": [{", ".join(fn(i) for i in v)}]' if len(v) else None
        return lambda v, fn=fn: (f'{{{", ".join(i for i in (f_i("added", v.added(), fn), f_i("removed", v.removed(), fn)) if i)}}}')

@to_json_converter.register(HgTSDTypeMetaData)
def _(value: HgTSDTypeMetaData, delta=False) -> Callable[[Any], str]:
    k_fn = to_json_converter(value.key_tp)
    if not issubclass(value.key_tp.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn=k_fn_inner: json.dumps(k_fn(k))  # escape the string
    v_fn = to_json_converter(value.value_tp, delta)
    f = lambda k, v, k_fn=k_fn, v_fn=v_fn: f'{k_fn(k)}: {v_fn(v)}' if v is not None else f'{k_fn(k)}: null'
    if not delta:
        return lambda v, f=f: f'{{{", ".join(f(i, t) for i, t in v.items())}}}'
    else:
        return lambda v, f=f: f'{{{", ".join(f(i, t) for i, t in chain(v.modified_items(), ((k, None) for k in v.removed_keys())))}}}'

def _td_to_str(delta: timedelta) -> str:
    if delta is None:
        return None
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    ms = delta.microseconds
    return f'"{days}:{hours}:{minutes}:{seconds}.{ms:06}"'


@singledispatch
def from_json_converter(value: HgTypeMetaData, delta=False) -> Callable[[dict], Any]:
    raise RuntimeError(f"Cannot convert to '{value}' from JSON")

@from_json_converter.register(HgTSTypeMetaData)
def _(value: HgTSTypeMetaData, delta=False) -> Callable[[Any], Any]:
    return from_json_converter(value.value_scalar_tp, delta)

@from_json_converter.register(HgCompoundScalarType)
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], Any]:
    fns = []
    for k, tp in value.meta_data_schema.items():
        fns.append((k, lambda v1, tp=tp, k=k: from_json_builder(tp, delta)(v1.get(k, None))))
    return lambda v2, fns=fns, tp=value.py_type: tp(**{k: v_ for k, fn in fns if (v_ := fn(v2)) is not None})

@from_json_converter.register(HgAtomicType)
def _(value: HgAtomicType, delta=False) -> Callable[[Any], Any]:
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
def _(value: HgDictScalarType, delta=False) -> Callable[[dict], Any]:
    k_fn = from_json_converter(value.key_type, delta)
    if not issubclass(value.key_type.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn=k_fn_inner: k_fn(json.loads(k))
    v_fn = from_json_converter(value.value_type, delta)
    return lambda v, k_fn=k_fn, v_fn=v_fn: {k_fn(k_): v_fn(v_) for k_, v_ in v.items()}


@from_json_converter.register(HgTupleCollectionScalarType)
def _(value: HgTupleCollectionScalarType, delta=False) -> Callable[[list], Any]:
    v_fn = from_json_converter(value.element_type, delta)
    return lambda v, v_fn=v_fn: tuple(v_fn(v_fn(i)) for i in v)


@from_json_converter.register(HgTSLTypeMetaData)
def _(value: HgTSLTypeMetaData, delta=False) -> Callable[[list], Any]:
    fn = from_json_converter(value.value_tp, delta)
    return lambda v, fn=fn: (
        {int(k): fn(i) for k, i in v.items()}
        if isinstance(v, dict)
        else tuple(fn(i) for i in v))


@from_json_converter.register(HgTSSTypeMetaData)
def _(value: HgTSSTypeMetaData, delta=False) -> Callable[[list], Any]:
    from hgraph._impl._types._tss import PythonSetDelta

    fn = from_json_converter(value.value_scalar_tp, delta)
    return lambda v, fn=fn: (
        PythonSetDelta(added={fn(i) for i in v.get("added", ())}, removed={fn(i) for i in v.get("removed", ())})
        if isinstance(v, dict)
        else tuple(fn(i) for i in v))


@from_json_converter.register(HgTSBTypeMetaData)
def _(value: HgTSBTypeMetaData, delta=False) -> Callable[[dict], Any]:
    schema = {}
    for k, tp in value.bundle_schema_tp.meta_data_schema.items():
        f = from_json_converter(tp, delta)
        schema[k] = lambda i, t, f=f: f(t)

    return lambda v, schema=schema: {i: schema[i](i, t) for i, t in v.items()}


@from_json_converter.register(HgTSDTypeMetaData)
def _(value: HgTSDTypeMetaData, delta=False) -> Callable[[dict], Any]:
    k_fn = from_json_converter(value.key_tp)
    if not issubclass(value.key_tp.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn=k_fn_inner: k_fn(json.loads(k))
    v_fn = from_json_converter(value.value_tp, delta)

    return lambda v, k_fn=k_fn, v_fn=v_fn: {k_fn(k): v_fn(v) if v is not None else REMOVE_IF_EXISTS for k, v in v.items()}


@compute_node(overloads=to_json)
def to_json_generic(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE, delta: bool = False) -> TS[str]:
    return to_json_builder(_tp, delta)(ts)


@compute_node(overloads=from_json)
def from_json_generic(ts: TS[str], _tp: type[OUT] = AUTO_RESOLVE, delta: bool = False) -> DEFAULT[OUT]:
    value = json.loads(ts.value)
    return from_json_builder(_tp, delta)(value)