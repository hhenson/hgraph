from typing import Tuple, Type

from hgraph._types._time_series_types import TIME_SERIES_TYPE, OUT
from hgraph._types._ts_type import TS
from hgraph._types._tsl_type import TSL
from hgraph._types._scalar_types import SIZE, DEFAULT
from hgraph._types._tsb_type import TimeSeriesSchema, TSB, TS_SCHEMA, TS_SCHEMA_1

from hgraph._wiring._decorators import operator

__all__ = ("str_", "match_", "Match", "replace", "split", "join", "format_", "substr")


@operator
def str_(ts: TIME_SERIES_TYPE) -> TS[str]:
    """
    Converts the incoming time series to a string representation. Default implementation would be str(ts.value).
    """


class Match(TimeSeriesSchema):
    is_match: TS[bool]
    groups: TS[Tuple[str, ...]]


@operator
def match_(pattern: TS[str], s: TS[str]) -> TSB[Match]:
    """
    Matches the pattern in the string and returns the result and matching groups.
    """


@operator
def replace(pattern: TS[str], repl: TS[str], s: TS[str]) -> TS[str]:
    """
    Replaces the pattern in the string with the replacement.
    """


@operator  # Would need to define substr in imports
def substr(s: TS[str], start: TS[int], end: TS[int] = None) -> TS[str]:
    """
    Extracts a substring from the input string time series based on start and end positions.

    :param s: Input string time series to perform extraction on.
    :param start: Starting position (inclusive) of the substring. Negative values count from the end
    :param end: Optional ending position (exclusive) of the substring. If None, extracts to the end of string.
    :return: Time series containing the extracted substring
    """


@operator
def split(s: TS[str], separator: str, maxsplits: int = -1, to: Type[OUT] = TS[Tuple[str, ...]]) -> DEFAULT[OUT]:
    """
    Splits the string over the separator into one of the given types:
     - TS[Tuple[str, ...]],
     - TS[Tuple[str, str]],
     - TSL[TS[str], SIZE]
    """


@operator
def join(*strings: TSL[TS[str], SIZE], separator: str) -> TS[str]:
    """
    Joins the strings with the separator.
    """


@operator
def format_(
    fmt: TS[str], *__pos_args__: TSB[TS_SCHEMA], __sample__: int = -1, **__kw_args__: TSB[TS_SCHEMA_1]
) -> TS[str]:
    """
    Writes the contents of the time-series values provided (in args / kwargs) to a string using the format string
    provided. The kwargs will be used as named inputs to the format string and args as enumerated args.
    :param fmt: A standard python format string (using {}). When converted to C++ this will use the c++ fmt
                       specifications.
    :param __sample__: set this to a positive value > 1 to only output every nth formatted string.
    :param args: Time series args
    :param kwargs: Time series kwargs
    :return: The formatted string.
    """
