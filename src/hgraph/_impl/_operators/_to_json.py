import json
from datetime import datetime, date, time, timedelta
from enum import Enum
from itertools import chain
from typing import Callable, Any

from multimethod import multimethod

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
    HgTSDTypeMetaData,
    REMOVE_IF_EXISTS,
    HgSetScalarType,
    set_delta,
)

__all__ = []

from hgraph._operators._to_json import to_json_builder, from_json_builder


def error_wrapper(fn: Callable[[Any], str], context: str) -> Callable[[Any], str]:
    def _fn(v):
        try:
            return None if v is None else fn(v)
        except Exception as e:
            raise RuntimeError(f"Error while converting {context} with value {v}:\n{e}") from e

    return _fn


@multimethod
def to_json_converter(value: HgTypeMetaData, delta=False) -> Callable[[Any], str]:
    raise RuntimeError(f"Cannot convert {value} to JSON")


def _compound_scalar_parent_encode(value: HgCompoundScalarType, delta: bool):
    switches = {v: to_json_converter(HgCompoundScalarType(v)) for v in value.py_type.__serialise_children__.values()}
    return lambda v: switches[type(v)](v)


@to_json_converter.register
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], str]:
    tp = value.py_type
    if tp.__serialise_base__:
        return _compound_scalar_parent_encode(value, delta)
    to_json = []
    if (f := tp.__serialise_discriminator_field__) is not None and f not in tp.__meta_data_schema__:
        to_json.append(
            error_wrapper(
                lambda v: (
                    f'"{v.__serialise_discriminator_field__}":'
                    f' "{getattr(v, v.__serialise_discriminator_field__, v.__class__.__name__)}"'
                ),
                f"{str(value)}: __serialise_discriminator_field__",
            )
        )
    for k, tp in value.meta_data_schema.items():
        m = to_json_converter(tp, delta)
        to_json.append(
            error_wrapper(
                lambda v, m_=m, k_=k: f'"{k_}": {m_(v_)}' if (v_ := getattr(v, k_, None)) is not None else "",
                f"{str(value)}: {k}:{str(tp)}",
            )
        )
    return error_wrapper(
        lambda v, to_json_=to_json: f'{{{", ".join(v_ for fn in to_json_ if (v_ := fn(v)))}}}', f"{str(value)}"
    )


@to_json_converter.register
def _(value: HgAtomicType, delta=False) -> Callable[[Any], str]:
    if issubclass(value.py_type, Enum):
        return lambda v: f'"{v.name}"'

    try:
        return {
            bool: lambda v: None if v is None else "true" if v else "false",
            int: lambda v: None if v is None else f"{v}",
            float: lambda v: None if v is None else f"{v}",
            str: lambda v: None if v is None else json.dumps(v),
            date: lambda v: None if v is None else f'"{v.strftime("%Y-%m-%d")}"',
            time: lambda v: None if v is None else f'"{v.strftime("%H:%M:%S.%f")}"',
            timedelta: _td_to_str,
            datetime: lambda v: None if v is None else f'"{v.strftime("%Y-%m-%d %H:%M:%S.%f")}"',
        }[value.py_type]
    except KeyError:
        raise RuntimeError(f"Cannot convert type: '{value}' to JSON")


@to_json_converter.register
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


@to_json_converter.register
def _(value: HgTupleCollectionScalarType, delta=False) -> Callable[[Any], str]:
    v_fn = to_json_converter(value.element_type, delta)
    return error_wrapper(lambda v, v_fn_=v_fn: f'[{", ".join(v_fn_(i) for i in v)}]', f"{str(value)}")


@to_json_converter.register
def _(value: HgTSTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp, delta)
    return lambda v, fn_=fn: fn_(v.value) if v.valid else ""


@to_json_converter.register
def _(value: HgTSLTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_tp, delta)

    def qstr(i):
        return f'"{str(i)}"'

    if delta:
        return lambda v, fn_=fn: f'{{{", ".join(qstr(i) + ": " + fn_(t) for i, t in v.modified_items())}}}'
    else:
        return lambda v, fn_=fn: f'[{", ".join(fn_(i) for i in v)}]'


@to_json_converter.register
def _(value: HgTSBTypeMetaData, delta=False) -> Callable[[Any], str]:
    schema = {}
    for k, tp in value.bundle_schema_tp.meta_data_schema.items():
        f = to_json_converter(tp, delta)
        schema[k] = lambda i, t, f_=f: f'"{i}": {f_(t)}'
    if delta:
        return lambda v, schema_=schema: f'{{{", ".join(schema_[i](i, t) for i, t in v.items())}}}'
    else:
        return lambda v, schema_=schema: f'{{{", ".join(schema_[i](i, t) for i, t in v.modified_items())}}}'


@to_json_converter.register
def _(value: HgTSSTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp, delta)
    if not delta:
        return lambda v, fn_=fn: f'[{", ".join(fn_(i) for i in v.values())}]'
    else:

        def f_i(a, v, f):
            return f'"{a}": [{", ".join(fn(i) for i in v)}]' if len(v) else None

        return lambda v, fn_=fn: (
            f'{{{", ".join(i for i in (f_i("added", v.added(), fn_), f_i("removed", v.removed(), fn_)) if i)}}}'
        )


@to_json_converter.register
def _(value: HgSetScalarType, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.element_type, delta)
    return lambda v, fn_=fn: f'[{", ".join(fn_(i) for i in v)}]'


@to_json_converter.register
def _(value: HgTSDTypeMetaData, delta=False) -> Callable[[Any], str]:
    k_fn = to_json_converter(value.key_tp)
    if not issubclass(value.key_tp.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn_=k_fn_inner: json.dumps(k_fn_(k))  # escape the string
    v_fn = to_json_converter(value.value_tp, delta)
    f = lambda k, v, k_fn_=k_fn, v_fn_=v_fn: f"{k_fn_(k)}: {v_fn_(v)}" if v is not None else f"{k_fn_(k)}: null"
    if not delta:
        return lambda v, f_=f: f'{{{", ".join(f_(i, t) for i, t in v.items())}}}'
    else:
        return lambda v, f_=f: (
            f'{{{", ".join(f_(i, t) for i, t in chain(v.modified_items(), ((k, None) for k in v.removed_keys())))}}}'
        )


def _td_to_str(delta: timedelta) -> str:
    if delta is None:
        return None
    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    ms = delta.microseconds
    return f'"{days}:{hours}:{minutes}:{seconds}.{ms:06}"'


@multimethod
def from_json_converter(value: HgTypeMetaData, delta=False) -> Callable[[dict], Any]:
    raise RuntimeError(f"Cannot convert to '{value}' from JSON")


@from_json_converter.register
def _(value: HgTSTypeMetaData, delta=False) -> Callable[[Any], Any]:
    return from_json_converter(value.value_scalar_tp, delta)


def _compound_scalar_parent_decode(value: HgCompoundScalarType, delta: bool):
    tp = value.py_type
    switches = {k: from_json_converter(HgCompoundScalarType(v)) for k, v in tp.__serialise_children__.items()}
    discriminator = tp.__serialise_discriminator_field__
    return lambda v, switches_=switches, d=discriminator: switches_[v.get(d)](v) if v is not None else None


@from_json_converter.register
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], Any]:
    if value.py_type.__serialise_base__:
        return _compound_scalar_parent_decode(value, delta)
    fns = []
    for k, tp in value.meta_data_schema.items():
        fns.append((
            k,
            error_wrapper(
                lambda v1, tp=tp, k=k: from_json_builder(tp, delta)(v1.get(k, None)), f"{str(value)} {k}: {str(tp)}"
            ),
        ))
    return error_wrapper(
        lambda v2, fns=fns, tp=value.py_type: tp(**{k: v_ for k, fn in fns if (v_ := fn(v2)) is not None}),
        f"{str(value)}",
    )


@from_json_converter.register
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


@from_json_converter.register
def _(value: HgDictScalarType, delta=False) -> Callable[[dict], Any]:
    k_fn = from_json_converter(value.key_type, delta)
    if not issubclass(value.key_type.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn_=k_fn_inner: k_fn_(json.loads(k))
    v_fn = from_json_converter(value.value_type, delta)
    return lambda v, k_fn_=k_fn, v_fn_=v_fn: {k_fn_(k_): v_fn_(v_) for k_, v_ in v.items()} if v is not None else None


@from_json_converter.register
def _(value: HgTupleCollectionScalarType, delta=False) -> Callable[[list], Any]:
    v_fn = from_json_converter(value.element_type, delta)
    return lambda v, v_fn_=v_fn: tuple(v_fn_(i) for i in v) if v is not None else None


@from_json_converter.register
def _(value: HgTSLTypeMetaData, delta=False) -> Callable[[list], Any]:
    fn = from_json_converter(value.value_tp, delta)
    return lambda v, fn_=fn: (
        ({int(k): fn_(i) for k, i in v.items()} if isinstance(v, dict) else tuple(fn_(i) for i in v))
        if v is not None
        else None
    )


@from_json_converter.register
def _(value: HgTSSTypeMetaData, delta=False) -> Callable[[list], Any]:
    fn = from_json_converter(value.value_scalar_tp, delta)
    tp = value.value_scalar_tp.py_type
    return lambda v, fn_=fn, _tp=tp: (
        (
            set_delta(
                added={fn_(i) for i in v.get("added", ())}, removed={fn_(i) for i in v.get("removed", ())}, tp=_tp
            )
            if isinstance(v, dict)
            else tuple(fn_(i) for i in v)
        )
        if v is not None
        else None
    )


@from_json_converter.register
def _(value: HgSetScalarType, delta=False) -> Callable[[list], Any]:
    fn = from_json_converter(value.element_type, delta)
    return lambda v, fn_=fn: frozenset(fn_(i) for i in v) if v is not None else None


@from_json_converter.register
def _(value: HgTSBTypeMetaData, delta=False) -> Callable[[dict], Any]:
    schema = {}
    for k, tp in value.bundle_schema_tp.meta_data_schema.items():
        f = from_json_converter(tp, delta)
        schema[k] = lambda i, t, f_=f: f_(t)

    return lambda v, schema_=schema: {i: schema_[i](i, t) for i, t in v.items()} if v is not None else None


@from_json_converter.register
def _(value: HgTSDTypeMetaData, delta=False) -> Callable[[dict], Any]:
    k_fn = from_json_converter(value.key_tp)
    if not issubclass(value.key_tp.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn=k_fn_inner: k_fn(json.loads(k))
    v_fn = from_json_converter(value.value_tp, delta)

    return lambda v, k_fn_=k_fn, v_fn_=v_fn: (
        {k_fn_(k): v_fn_(v) if v is not None else REMOVE_IF_EXISTS for k, v in v.items()} if v is not None else None
    )


@compute_node(overloads=to_json)
def to_json_generic(ts: TIME_SERIES_TYPE, _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE, delta: bool = False) -> TS[str]:
    return to_json_builder(_tp, delta)(ts)


@compute_node(overloads=from_json)
def from_json_generic(ts: TS[str], _tp: type[OUT] = AUTO_RESOLVE, delta: bool = False) -> DEFAULT[OUT]:
    value = json.loads(ts.value)
    return from_json_builder(_tp, delta)(value)
