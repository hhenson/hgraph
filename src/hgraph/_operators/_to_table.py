from dataclasses import dataclass
from datetime import datetime

from hgraph._operators._operators import operator
from hgraph._types import TIME_SERIES_TYPE, CompoundScalar, TS, SCALAR
from hgraph._runtime import GlobalState, EvaluationClock

__all__ = (
    "to_table",
    "to_table_schema",
    "TableSchema",
    "set_as_of",
    "get_as_of",
    "set_as_of_key",
    "set_date_key",
    "make_table_schema",
)


DATE_KEY = "__to_table__::date_key"
AS_OF_KEY = "__to_table__::as_of_key"
AS_OF_VALUE = "__to_table__::as_of"


def set_as_of(dt: datetime):
    """
    Set the as_of time. This will ensure this is the time used for all as_of entries.
    """
    GlobalState.instance()[AS_OF_VALUE] = dt


def set_as_of_key(key: str):
    GlobalState.instance()[AS_OF_KEY] = key


def set_date_key(key: str):
    GlobalState.instance()[DATE_KEY] = key


def get_as_of(clock: EvaluationClock) -> datetime:
    """
    Returns the as_of time
    """
    if dt := GlobalState.instance().get(AS_OF_VALUE, None):
        return dt
    else:
        return clock.now


@dataclass(frozen=True)
class TableSchema(CompoundScalar):
    keys: tuple[str, ...]
    types: tuple[type, ...]
    partition_keys: tuple[str, ...]  # An empty set implies a single row per tick.
    date_time_key: str
    as_of_key: str  # Empty string when no as_of_key is present


def make_table_schema(
    keys: tuple[str, ...],
    types: tuple[type, ...],
    partition_keys: tuple[str, ...] = tuple(),
    date_key: str = None,
    as_of_key: str = None,
) -> TableSchema:
    if date_key is None:
        date_key = GlobalState.instance().get(DATE_KEY, "__date_time__")

    if as_of_key is None:
        as_of_key = GlobalState.instance().get(AS_OF_KEY, "__as_of__")

    keys_ = [date_key, as_of_key]
    types_ = [datetime, datetime]

    keys_.extend(keys)
    types_.extend(types)

    return TableSchema(
        keys=tuple(keys_),
        types=tuple(types_),
        partition_keys=partition_keys,
        date_time_key=date_key,
        as_of_key=as_of_key,
    )


@operator
def to_table(ts: TIME_SERIES_TYPE) -> TS[SCALAR]:
    """
    Convert the incoming time-series value to a tabular form.
    The result includes a schema description that represents the shape
    """


@operator
def to_table_schema(tp: type[TIME_SERIES_TYPE]) -> TS[TableSchema]:
    """
    A const tick of the expected schema that will be produced by the to_table operator.
    """
