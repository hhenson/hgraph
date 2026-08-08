from functools import cache
from typing import Any

from hgraph._types import TIME_SERIES_TYPE, TS, OUT, HgTypeMetaData
from hgraph._types._scalar_types import DEFAULT
from hgraph._wiring._decorators import operator

__all__ = ["to_json", "from_json", "to_json_builder", "from_json_builder", "register_json_datetime_format"]


def register_json_datetime_format(fmt: str, *, time_only: bool = False) -> None:
    """
    Add a ``strptime`` pattern to those accepted when reading a date, time or datetime from JSON.

    Dates and times are written as ISO 8601 and are read as ISO 8601 first, which already covers a
    ``T`` or space separator, a ``Z`` suffix, numeric offsets and optional fractional seconds. Use
    this only for a source that emits something else:

    ::

        register_json_datetime_format("%d/%m/%Y")

    Patterns are tried in registration order. An ambiguous pattern captures every value matching
    it, so register only what the producer actually emits.

    :param fmt: the ``strptime`` pattern to accept.
    :param time_only: register against times rather than dates and datetimes.
    """
    from hgraph._impl._operators._to_json import register_json_datetime_format as _register

    _register(fmt, time_only=time_only)


@operator
def to_json(ts: DEFAULT[TIME_SERIES_TYPE], delta: bool = False) -> TS[str]:
    """
    Converts the ``ts`` to a JSON string.
    """
    ...


@operator
def from_json(ts: TS[str]) -> DEFAULT[OUT]:
    """
    Converts the ``ts`` JSON string to the OUT type.
    Usage would be along the lines of:

    ::

        value = from_json[TS[MySchema]](ts)

    """
    ...


@cache
def to_json_builder(tp: TIME_SERIES_TYPE, delta=False) -> Any:
    """
    Creates a builder that will convert the value from an instance of the time-series to a string value.
    """
    from hgraph._impl._operators._to_json import to_json_converter

    return to_json_converter(HgTypeMetaData.parse_type(tp), delta)


@cache
def from_json_builder(tp: type[TIME_SERIES_TYPE], delta=False) -> Any:
    """
    Creates a builder that will convert a string value to a value suitable to value to feed an output of the time-series.
    """
    from hgraph._impl._operators._to_json import from_json_converter

    return from_json_converter(HgTypeMetaData.parse_type(tp), delta)
