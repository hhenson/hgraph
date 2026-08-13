"""Cross-runtime canonicalization without importing either hgraph package."""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import enum
import json
import math
from collections.abc import Mapping
from typing import Any


CANONICAL_SCHEMA_VERSION = 1


def _sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _canonical_float(value: float) -> dict[str, str]:
    if math.isnan(value):
        encoded = "nan"
    elif math.isinf(value):
        encoded = "inf" if value > 0 else "-inf"
    else:
        encoded = value.hex()
    return {"$float": encoded}


def canonicalize(value: Any) -> Any:
    """Convert a runtime value to a stable JSON-compatible representation."""

    # IntEnum and StrEnum are also instances of their primitive base classes;
    # preserve nominal identity before the primitive fast path so differential
    # recipes can detect enum schema loss at the Python boundary.
    if isinstance(value, enum.Enum):
        return {
            "$enum": {
                "type": type(value).__qualname__,
                "name": value.name,
                "value": canonicalize(value.value),
            }
        }
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return _canonical_float(value)
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dt.datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, dt.date):
        return {"$date": value.isoformat()}
    if isinstance(value, dt.timedelta):
        return {
            "$timedelta": {
                "days": value.days,
                "seconds": value.seconds,
                "microseconds": value.microseconds,
            }
        }

    type_name = type(value).__name__
    if type_name == "Sentinel" and getattr(value, "name", None) == "REMOVE":
        return {"$remove": True}
    if (
        type_name == "Sentinel"
        and getattr(value, "name", None) == "REMOVE_IF_EXISTS"
    ):
        return {"$remove_if_exists": True}
    if type_name in {"_Removed", "Removed"} and repr(value) == "REMOVE":
        return {"$remove": True}
    if type_name in {"_RemovedIfExists", "RemovedIfExists"} and repr(value) == "REMOVE_IF_EXISTS":
        return {"$remove_if_exists": True}
    if type_name == "Removed" and hasattr(value, "item"):
        return {"$set_removed": canonicalize(value.item)}
    if hasattr(value, "added") and hasattr(value, "removed") and (
        "SetDelta" in type_name or type_name == "_SetDelta"
    ):
        added = sorted((canonicalize(item) for item in value.added), key=_sort_key)
        removed = sorted((canonicalize(item) for item in value.removed), key=_sort_key)
        return {"$set_delta": {"added": added, "removed": removed}}

    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        fields = [
            [field.name, canonicalize(getattr(value, field.name))]
            for field in dataclasses.fields(value)
        ]
        return {"$dataclass": {"type": type(value).__qualname__, "fields": fields}}
    if isinstance(value, Mapping):
        entries = [
            [canonicalize(key), canonicalize(item)] for key, item in value.items()
        ]
        entries.sort(key=lambda pair: _sort_key(pair[0]))
        return {"$map": entries}
    if isinstance(value, tuple):
        return {"$tuple": [canonicalize(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        items = sorted((canonicalize(item) for item in value), key=_sort_key)
        return {"$set": items}
    if isinstance(value, list):
        return [canonicalize(item) for item in value]

    if hasattr(value, "__dict__"):
        fields = [
            [str(key), canonicalize(item)]
            for key, item in sorted(vars(value).items(), key=lambda pair: str(pair[0]))
            if not str(key).startswith("_")
        ]
        if fields:
            return {"$object": {"type": type(value).__qualname__, "fields": fields}}
    return {"$repr": {"type": type(value).__qualname__, "value": repr(value)}}


def canonical_json(value: Any) -> str:
    return json.dumps(
        canonicalize(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
