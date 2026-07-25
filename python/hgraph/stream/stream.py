import re
import types
import typing
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import Generic

from hgraph import COMPOUND_SCALAR, CompoundScalar, K, SCALAR, SIZE, TS, TSB, TSD, TSL, TimeSeriesSchema, compute_node
from hgraph._types import _substitute_typevars
from hgraph._wiring import _unwrap

__all__ = (
    "Data",
    "StreamStatus",
    "Stream",
    "combine_statuses",
    "combine_status_messages",
    "combine_status_messages_",
    "merge_join",
    "register_status_message_pattern",
    "reduce_statuses",
    "reduce_status_messages",
)


class StreamStatus(Enum):
    OK = 0
    STALE = 1
    WAITING = 2
    NA = 3
    ERROR = 4
    FATAL = 5


@dataclass(frozen=True)
class Data(CompoundScalar, Generic[SCALAR]):
    values: SCALAR
    timestamp: datetime


_STREAM_SCHEMAS = {}


def _schema_token(value):
    schema = getattr(value, "schema", None)
    if isinstance(schema, type):
        return f"{schema.__module__}.{schema.__qualname__}"
    origin = typing.get_origin(value)
    if origin is not None:
        arguments = ",".join(_schema_token(argument) for argument in typing.get_args(value))
        return f"{origin.__module__}.{origin.__qualname__}[{arguments}]"
    if isinstance(value, type):
        return f"{value.__module__}.{value.__qualname__}"
    return repr(value)


class Stream:
    """Build a status-prefixed bundle around a scalar or TS schema payload."""

    def __class_getitem__(cls, payload):
        cached = _STREAM_SCHEMAS.get(payload)
        if cached is not None:
            return cached

        origin = typing.get_origin(payload) or payload
        is_open_compound = payload is COMPOUND_SCALAR
        is_compound = (
            isinstance(origin, type) and issubclass(origin, CompoundScalar)
        )
        is_ts_schema = (
            isinstance(origin, type) and issubclass(origin, TimeSeriesSchema)
        )
        # RFC 0004 python-owned structured scalars: a plain (frozen)
        # dataclass is a structured payload too; its fields resolve through
        # the public reflection layer exactly like a CompoundScalar's.
        is_python_owned = (
            not is_compound and not is_ts_schema
            and isinstance(origin, type) and is_dataclass(origin)
        )
        if (not is_open_compound and not is_compound and not is_ts_schema
                and not is_python_owned):
            raise TypeError(
                "Stream[...] requires a CompoundScalar, TimeSeriesSchema, or "
                f"structured dataclass payload, got {payload!r}"
            )
        parameters = tuple(getattr(origin, "__parameters__", ())) if not is_open_compound else ()
        arguments = tuple(typing.get_args(payload))
        if arguments and len(arguments) != len(parameters):
            raise TypeError(
                f"{origin.__qualname__} expects {len(parameters)} type arguments, "
                f"got {len(arguments)}"
            )
        substitutions = dict(zip(parameters, arguments))
        annotations = {
            "status": TS[StreamStatus],
            "status_msg": TS[str],
        }
        if is_compound:
            resolved_annotations = typing.get_type_hints(origin)
            for item in fields(origin):
                annotations[item.name] = TS[
                    _substitute_typevars(
                        resolved_annotations.get(item.name, item.type), substitutions)
                ]
        elif is_ts_schema or is_python_owned:
            from hgraph.reflection import fields as reflected_fields

            annotations.update(reflected_fields(TSB[payload]))

        token = _schema_token(payload)
        name = f"Stream[{token}]"
        schema = types.new_class(name, (TimeSeriesSchema,))
        schema.__module__ = __name__
        schema.__annotations__ = annotations
        _STREAM_SCHEMAS[payload] = schema
        return schema


_STATUS_MESSAGE_PATTERNS = []


def register_status_message_pattern(pattern: str):
    """Register a one-group message pattern whose identifiers can be merged."""
    try:
        prefix, suffix = pattern.split(r"(\w+)", maxsplit=1)
    except ValueError as error:
        raise ValueError("status message pattern must contain one '(\\w+)' group") from error

    def escape(value):
        return value.replace("(", r"\(").replace(")", r"\)")

    _STATUS_MESSAGE_PATTERNS.append(
        (f"^{escape(prefix)}(.*){escape(suffix)}$", prefix, suffix)
    )


def _dedup_components(pattern, prefix, suffix, components):
    grouped = []
    remaining = set()
    for component in components:
        match = re.search(pattern, component) if component else None
        if match is None:
            if component:
                remaining.add(component)
        else:
            grouped.extend(match.group(1).split(", "))
    if grouped:
        remaining.add(f"{prefix}{', '.join(sorted(set(grouped)))}{suffix}")
    return remaining


def combine_status_messages_(message1: str | None, message2: str | None) -> str:
    if not message1:
        return message2 or ""
    if not message2:
        return message1
    if message1 in message2:
        return message2
    if message2 in message1:
        return message1

    components = set(message1.split("; ") + message2.split("; "))
    for pattern, prefix, suffix in _STATUS_MESSAGE_PATTERNS:
        components = _dedup_components(pattern, prefix, suffix, components)
    return "; ".join(sorted(components))


@compute_node(valid=())
def combine_status_messages(message1: TS[str], message2: TS[str]) -> TS[str]:
    lhs = message1.value if message1.valid else None
    rhs = message2.value if message2.valid else None
    return combine_status_messages_(lhs, rhs)


@compute_node(valid=())
def combine_statuses(
    status1: TS[StreamStatus],
    status2: TS[StreamStatus],
) -> TS[StreamStatus]:
    lhs = status1.value if status1.valid else StreamStatus.WAITING
    rhs = status2.value if status2.valid else StreamStatus.WAITING
    return max(lhs, rhs, key=lambda status: status.value)


@compute_node(valid=())
def merge_join(str1: TS[str], str2: TS[str], separator: str) -> TS[str]:
    lhs = str1.value if str1.valid else None
    rhs = str2.value if str2.valid else None
    if lhs is None:
        return rhs
    if rhs is None:
        return lhs
    if not lhs and not rhs:
        return ""
    return separator.join(
        sorted(
            {
                piece
                for piece in chain(lhs.strip().split(separator), rhs.strip().split(separator))
                if piece
            }
        )
    )


@compute_node(valid=())
def _reduce_statuses_tsd(values: TSD[K, TS[StreamStatus]]) -> TS[StreamStatus]:
    statuses = values.value.values() if values.valid else ()
    return max(statuses, key=lambda status: status.value, default=StreamStatus.WAITING)


@compute_node(valid=())
def _reduce_statuses_tsl(values: TSL[TS[StreamStatus], SIZE]) -> TS[StreamStatus]:
    statuses = (value for value in values.value if value is not None) if values.valid else ()
    return max(statuses, key=lambda status: status.value, default=StreamStatus.WAITING)


@compute_node(valid=())
def _reduce_messages_tsd(values: TSD[K, TS[str]]) -> TS[str]:
    result = ""
    for message in values.value.values() if values.valid else ():
        result = combine_status_messages_(result, message)
    return result


@compute_node(valid=())
def _reduce_messages_tsl(values: TSL[TS[str], SIZE]) -> TS[str]:
    result = ""
    for message in values.value if values.valid else ():
        result = combine_status_messages_(result, message)
    return result


def reduce_statuses(values):
    ts_type = _unwrap(values).ts_type
    if ts_type.is_tsd:
        return _reduce_statuses_tsd(values)
    if ts_type.is_tsl:
        return _reduce_statuses_tsl(values)
    raise TypeError("reduce_statuses requires a TSD or TSL of StreamStatus")


def reduce_status_messages(values):
    ts_type = _unwrap(values).ts_type
    if ts_type.is_tsd:
        return _reduce_messages_tsd(values)
    if ts_type.is_tsl:
        return _reduce_messages_tsl(values)
    raise TypeError("reduce_status_messages requires a TSD or TSL of str")
