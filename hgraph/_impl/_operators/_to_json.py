import json
from datetime import datetime, date, time, timedelta, timezone
from enum import Enum
from itertools import chain
from typing import Callable, Any, get_origin

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


def error_wrapper(fn: Callable[[Any], str], context: str, none_value: Any = None) -> Callable[[Any], str]:
    def _fn(v):
        try:
            return none_value if v is None else fn(v)
        except Exception as e:
            raise RuntimeError(f"Error while converting {context} with value {v}:\n{e}") from e

    return _fn


@multimethod
def to_json_converter(value: HgTypeMetaData, delta=False) -> Callable[[Any], str]:
    raise RuntimeError(f"Cannot convert {value} to JSON")


def _compound_scalar_parent_encode(value: HgCompoundScalarType, delta: bool):
    tp = get_origin(value.py_type) or value.py_type
    switches = {
        v: to_json_converter(HgCompoundScalarType(v)) for v in getattr(tp, "__serialise_children__", {}).values()
    }
    return lambda v: "null" if v is None else switches[type(v)](v)


@to_json_converter.register
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], str]:
    tp = value.py_type
    origin = get_origin(tp) or tp
    if getattr(origin, "__serialise_base__", False):
        return _compound_scalar_parent_encode(value, delta)
    to_json = []
    if (
        f := getattr(origin, "__serialise_discriminator_field__", None)
    ) is not None and f not in value.meta_data_schema:
        to_json.append(
            error_wrapper(
                lambda v, f_=f: f'"{f_}": "{getattr(v, f_, v.__class__.__name__)}"',
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
        lambda v, to_json_=to_json: f'{{{", ".join(v_ for fn in to_json_ if (v_ := fn(v)))}}}',
        f"{str(value)}",
        none_value="null",
    )


@to_json_converter.register
def _(value: HgAtomicType, delta=False) -> Callable[[Any], str]:
    if issubclass(value.py_type, Enum):
        return lambda v: "null" if v is None else f'"{v.name}"'

    try:
        return {
            bool: lambda v: "null" if v is None else "true" if v else "false",
            int: lambda v: "null" if v is None else f"{v}",
            float: lambda v: "null" if v is None else f"{v}",
            str: lambda v: "null" if v is None else json.dumps(v),
            # ISO 8601, which is what the C++ engine emits: a 'T' separator, and fractional
            # seconds only when they are non-zero. `isoformat` produces exactly that.
            date: lambda v: "null" if v is None else f'"{v.isoformat()}"',
            time: lambda v: "null" if v is None else f'"{v.isoformat()}"',
            timedelta: _td_to_str,
            datetime: lambda v: "null" if v is None else f'"{v.isoformat()}"',
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
        if v is None:
            return "null"
        items = (f"{k_fn(k)}: {v_fn(v_)}" for k, v_ in v.items())
        return f'{{{", ".join(items)}}}'

    return _to_json


@to_json_converter.register
def _(value: HgTupleCollectionScalarType, delta=False) -> Callable[[Any], str]:
    v_fn = to_json_converter(value.element_type, delta)
    return error_wrapper(
        lambda v, v_fn_=v_fn: f'[{", ".join(v_fn_(i) for i in v)}]', f"{str(value)}", none_value="null"
    )


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
        return lambda v, fn_=fn: (
            "null" if v is None else f'{{{", ".join(qstr(i) + ": " + fn_(t) for i, t in v.modified_items())}}}'
        )
    else:
        return lambda v, fn_=fn: "null" if v is None else f'[{", ".join(fn_(i) for i in v)}]'


@to_json_converter.register
def _(value: HgTSBTypeMetaData, delta=False) -> Callable[[Any], str]:
    schema = {}
    for k, tp in value.bundle_schema_tp.meta_data_schema.items():
        f = to_json_converter(tp, delta)
        schema[k] = lambda i, t, f_=f: f'"{i}": {f_(t)}'
    if delta:
        return lambda v, schema_=schema: (
            "null" if v is None else f'{{{", ".join(schema_[i](i, t) for i, t in v.items())}}}'
        )
    else:
        return lambda v, schema_=schema: (
            "null" if v is None else f'{{{", ".join(schema_[i](i, t) for i, t in v.modified_items())}}}'
        )


@to_json_converter.register
def _(value: HgTSSTypeMetaData, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.value_scalar_tp, delta)
    if not delta:
        return lambda v, fn_=fn: "null" if v is None else f'[{", ".join(fn_(i) for i in v.values())}]'
    else:

        def f_i(a, v, f):
            return f'"{a}": [{", ".join(fn(i) for i in v)}]' if len(v) else None

        return lambda v, fn_=fn: (
            "null"
            if v is None
            else f'{{{", ".join(i for i in (f_i("added", v.added(), fn_), f_i("removed", v.removed(), fn_)) if i)}}}'
        )


@to_json_converter.register
def _(value: HgSetScalarType, delta=False) -> Callable[[Any], str]:
    fn = to_json_converter(value.element_type, delta)
    return lambda v, fn_=fn: "null" if v is None else f'[{", ".join(fn_(i) for i in v)}]'


@to_json_converter.register
def _(value: HgTSDTypeMetaData, delta=False) -> Callable[[Any], str]:
    k_fn = to_json_converter(value.key_tp)
    if not issubclass(value.key_tp.py_type, (str, date, time, timedelta, datetime)):
        k_fn_inner = k_fn
        k_fn = lambda k, k_fn_=k_fn_inner: json.dumps(k_fn_(k))  # escape the string
    v_fn = to_json_converter(value.value_tp, delta)

    def f(k, v, k_fn_=k_fn, v_fn_=v_fn):
        return f"{k_fn_(k)}: {v_fn_(v)}" if v is not None else f"{k_fn_(k)}: null"

    if not delta:
        return lambda v, f_=f: "null" if v is None else f'{{{", ".join(f_(i, t) for i, t in v.items())}}}'
    else:

        def _to_json(v, f_=f):
            if v is None:
                return "null"
            items = chain(v.modified_items(), ((k, None) for k in v.removed_keys()))
            return f'{{{", ".join(f_(i, t) for i, t in items)}}}'

        return _to_json


def _td_to_str(delta: timedelta) -> str:
    if delta is None:
        return "null"
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
    origin = get_origin(tp) or tp
    switches = {
        k: from_json_converter(HgCompoundScalarType(v))
        for k, v in getattr(origin, "__serialise_children__", {}).items()
    }
    discriminator = getattr(origin, "__serialise_discriminator_field__", None)
    return lambda v, switches_=switches, d=discriminator: switches_[v.get(d)](v) if v is not None else None


@from_json_converter.register
def _(value: HgCompoundScalarType, delta=False) -> Callable[[Any], Any]:
    if getattr(get_origin(value.py_type) or value.py_type, "__serialise_base__", False):
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
        date: parse_json_date,
        time: parse_json_time,
        timedelta: _str_to_td,
        datetime: parse_json_datetime,
    }.get(value.py_type, lambda v: v)


# Additional patterns tried, in order, when a value is not ISO 8601. ISO itself is not listed:
# `fromisoformat` already accepts it, including a 'T' or ' ' separator, a 'Z' suffix, numeric
# offsets, the basic (unpunctuated) forms and fractional seconds of any length -- so the previously
# emitted "%Y-%m-%d %H:%M:%S.%f" still reads back. These cover the common non-ISO renderings; only
# unambiguous ones are included, since a bare "01/02/2024" cannot be resolved without knowing the
# producer's convention. Use ``register_json_datetime_format`` to add your own.
_JSON_DATETIME_FORMATS: list[str] = [
    "%Y/%m/%d %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%d-%b-%Y %H:%M:%S.%f",
    "%d-%b-%Y %H:%M:%S",
    "%d-%b-%Y",
    "%d %b %Y %H:%M:%S",
    "%d %b %Y",
]

_JSON_TIME_FORMATS: list[str] = []

# Compact, unpunctuated values are chosen by length rather than by trying patterns in order.
# Neither of the general mechanisms gets these right on its own:
#
#   * `strptime` lets a variable-width directive take fewer digits than intended, and two adjacent
#     numeric directives have no boundary between them. "%Y%m%d%H%M%S%f" reads 20240613101530 as
#     10:15:03 -- %S takes one digit and %f swallows the rest -- so a pattern list silently
#     corrupts every fractionless value it reaches.
#   * `fromisoformat` mis-reads the 20-digit form: 20240613101530123456 comes back as
#     01:53:01.234560. It handles the 'T'-separated spelling correctly, but not this one.
#
# Length is unambiguous for these three shapes, so it decides, and it is applied before ISO so the
# second case above cannot arise.
_COMPACT_DATETIME_FORMATS: dict[int, str] = {8: "%Y%m%d", 14: "%Y%m%d%H%M%S", 20: "%Y%m%d%H%M%S%f"}
_COMPACT_TIME_FORMATS: dict[int, str] = {6: "%H%M%S", 12: "%H%M%S%f"}


def _parse_compact(v: str, formats: dict[int, str]) -> datetime:
    if not (isinstance(v, str) and v.isdigit()) or (fmt := formats.get(len(v))) is None:
        raise ValueError("not a compact date or time")
    return datetime.strptime(v, fmt)


def register_json_datetime_format(fmt: str, *, time_only: bool = False) -> None:
    """
    Add a ``strptime`` pattern to those accepted when reading a date, time or datetime from JSON.

    Patterns are tried in registration order, after ISO 8601. Registering an ambiguous pattern such
    as ``"%d/%m/%Y"`` makes every value matching it parse that way, so register only what the
    producer actually emits.

    :param fmt: the ``strptime`` pattern to accept.
    :param time_only: register against times rather than dates and datetimes.
    """
    formats = _JSON_TIME_FORMATS if time_only else _JSON_DATETIME_FORMATS
    if fmt not in formats:
        formats.append(fmt)


def _as_naive_utc(v: datetime) -> datetime:
    """The engine works in naive UTC, so an offset in the source is applied and then dropped."""
    return v.astimezone(timezone.utc).replace(tzinfo=None) if v.tzinfo is not None else v


def _compact_datetime(v: str) -> datetime:
    return _parse_compact(v, _COMPACT_DATETIME_FORMATS)


def _compact_time(v: str) -> datetime:
    return _parse_compact(v, _COMPACT_TIME_FORMATS)


def _parse(v: str, iso: tuple[Callable[[str], Any], ...], formats: list[str], what: str):
    for parse in (*iso, *(lambda s, f=f: datetime.strptime(s, f) for f in formats)):
        try:
            return parse(v)
        except (TypeError, ValueError):
            continue
    raise ValueError(f"Cannot parse '{v}' as a {what}; it is not ISO 8601 nor any registered format")


def parse_json_datetime(v) -> datetime:
    """Parse a datetime from JSON, accepting ISO 8601 and any registered format."""
    if v is None or isinstance(v, datetime):
        return _as_naive_utc(v) if v is not None else None
    return _as_naive_utc(_parse(v, (_compact_datetime, datetime.fromisoformat), _JSON_DATETIME_FORMATS, "datetime"))


def parse_json_date(v) -> date:
    """Parse a date from JSON, accepting ISO 8601 and any registered format."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    parsed = _parse(v, (_compact_datetime, date.fromisoformat, datetime.fromisoformat), _JSON_DATETIME_FORMATS, "date")
    return parsed.date() if isinstance(parsed, datetime) else parsed


def parse_json_time(v) -> time:
    """Parse a time from JSON, accepting ISO 8601 and any registered format."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.time()
    if isinstance(v, time):
        return v
    parsed = _parse(v, (_compact_time, time.fromisoformat), _JSON_TIME_FORMATS, "time")
    return parsed.time() if isinstance(parsed, datetime) else parsed


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
