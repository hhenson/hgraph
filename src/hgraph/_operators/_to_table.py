from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto
from operator import le
from typing import TypeVar, Callable, Any

from hgraph._operators._operators import operator
from hgraph._runtime._global_state import GlobalState
from hgraph._runtime._evaluation_clock import EvaluationClock
from hgraph._types import TIME_SERIES_TYPE, CompoundScalar, TS, OUT, DEFAULT
from hgraph._types._scalar_type_meta_data import HgTupleFixedScalarType, HgTupleCollectionScalarType
from hgraph._wiring._wiring_errors import CustomMessageWiringError

__all__ = (
    "to_table",
    "from_table",
    "table_schema",
    "table_shape",
    "table_shape_from_schema",
    "shape_of_table_type",
    "TableSchema",
    "set_as_of",
    "get_as_of",
    "set_table_schema_as_of_key",
    "set_table_schema_date_key",
    "get_table_schema_as_of_key",
    "get_table_schema_date_key",
    "make_table_schema",
    "TABLE",
)


DATE_KEY = "__to_table__::date_key"
AS_OF_KEY = "__to_table__::as_of_key"
AS_OF_VALUE = "__to_table__::as_of"
DEL_KEY = "__to_table__::del_key"
TABLE = TypeVar("TABLE", bound=tuple)


def set_as_of(dt: datetime):
    """
    Set the as_of time. This will ensure this is the time used for all as_of entries.
    """
    GlobalState.instance()[AS_OF_VALUE] = dt


def get_as_of(clock: EvaluationClock | None = None) -> datetime:
    """
    Returns the as_of time
    """
    if dt := GlobalState.instance().get(AS_OF_VALUE, None):
        return dt
    else:
        return clock.now if clock else datetime.utcnow()


@dataclass(frozen=True)
class TableSchema(CompoundScalar):
    tp: type
    keys: tuple[str, ...]
    types: tuple[type, ...]
    partition_keys: tuple[str, ...]  # An empty set implies a single row per tick.
    removed_keys: tuple[str, ...]  # Only present when there are partition_keys.
    date_time_key: str
    as_of_key: str


@dataclass(frozen=True)
class TableOptions(CompoundScalar):
    key_names: tuple[str, ...]
    date_time_key: str
    as_of_key: str


class ToTableMode(Enum):
    Tick = auto()  # Write only modifies values into the table, if a time series was not modified, write None
    Sample = (
        auto()
    )  # Write all values to the table, but only for TSD keys where values were modified, also write key removals
    Snap = auto()  # Write all values to the table, modified of not, does not write removals


def table_shape(ts: type[TIME_SERIES_TYPE]) -> TABLE:
    """The table shape from the time-series type"""
    schema = table_schema(ts).value
    return table_shape_from_schema(schema)


def table_shape_from_schema(schema: TableSchema) -> TABLE:
    """The shape of the table from the schema"""
    row = schema.types
    if schema.partition_keys:
        return tuple[tuple[*row], ...]
    else:
        return tuple[*row]


def get_table_schema_date_key() -> str:
    """
    The date key used for table structures. (The date column is of datetime type)
    The default value is ``__date_time__``.
    """
    return (GlobalState.instance() if GlobalState.has_instance() else {}).get(DATE_KEY, "__date_time__")


def get_table_schema_as_of_key() -> str:
    """
    The as_of key used for table structures. This represents when the entry was last updated.
    The default value is ``__as_of_key__``.
    """
    return (GlobalState.instance() if GlobalState.has_instance() else {}).get(AS_OF_KEY, "__as_of__")


def set_table_schema_as_of_key(key: str):
    """
    Set the column name to be used for the as_of column.
    """
    GlobalState.instance()[AS_OF_KEY] = key


def set_table_schema_date_key(key: str):
    """
    Set the column name to be used for the date column. (Where date is actually datetime)
    """
    GlobalState.instance()[DATE_KEY] = key


def make_table_schema(
    tp: type,
    keys: tuple[str, ...],
    types: tuple[type, ...],
    partition_keys: tuple[str, ...] = tuple(),
    removed_keys: tuple[str, ...] = tuple(),
    date_key: str = None,
    as_of_key: str = None,
) -> TableSchema:
    """
    Constructs the table schema. This ensures we use the system default behaviour when
    date_key, as_of_key and removed_key are not set. It also adds the required date_key, as_of_key
    and removed_key as required, setting the default times.

    If it is cheap to call ``to_table_const`` with the time-series delta-value,
    then only the ``to_table_const`` is required.
    This will generate the ``to_table`` function with the delta value supplied.

    If the serialisation is to be handled withing a dedicated overload, then the converter functions are not required.
    """
    if date_key is None:
        date_key = get_table_schema_date_key()

    if as_of_key is None:
        as_of_key = get_table_schema_as_of_key()

    keys_ = [date_key, as_of_key]
    types_ = [datetime, datetime]

    keys_.extend(keys)
    types_.extend(types)

    return TableSchema(
        tp=tp,
        keys=tuple(keys_),
        types=tuple(types_),
        partition_keys=partition_keys,
        removed_keys=removed_keys,
        date_time_key=date_key,
        as_of_key=as_of_key,
    )


@operator
def table_schema(tp: type[TIME_SERIES_TYPE]) -> TS[TableSchema]:
    """
    A const tick of the expected schema that will be produced by the to_table operator.
    """


@operator
def to_table(ts: TIME_SERIES_TYPE, mode: TS[ToTableMode] = ToTableMode.Tick) -> TS[TABLE]:
    """
    Convert the incoming time-series value to a tabular form.
    The result includes a schema description that represents the shape
    """


@operator
def from_table(ts: TS[TABLE]) -> DEFAULT[OUT]:
    """
    Extracts data from a tabular form into the appropriate output type.
    The output type must be specified, and the input schema must be correctly structured for the type to be
    extracted.

    This reverses the ``to_table`` operator.
    The Schema can be obtained using the ``table_schema`` operator.
    """


@operator
def from_table_const(value: TABLE) -> DEFAULT[OUT]:
    """
    Extract data from a table tuple into the appropriate output type.
    This is the constant version of the from_table operator.
    """


def shape_of_table_type(
    tp: type[TABLE | HgTupleFixedScalarType | HgTupleCollectionScalarType],
    expect_keys: bool | None = None,
    expect_length: int | None = None,
) -> tuple[tuple[type, ...], bool]:
    """
    Extract the tuple shape from the type. To help with validation, it is possible to provide the ``expected_keys``
    to indicate if the shape is expected to have keys (=> tuple[tuple[type, ...], ...]). If the length of
    the row tuple is known, it can be provided using the ``expect_length`` parameter. This will ensure that the length
    (other than the date and as-of, etc. fields) is as specified.
    """
    if isinstance(tp, type):
        if issubclass(tp, tuple):
            tp = HgTupleFixedScalarType.parse_type(tp)
        else:
            raise CustomMessageWiringError(f"shape_of_table_type{repr(tp)} should be a tuple type")
    elif not isinstance(tp, (HgTupleFixedScalarType, HgTupleCollectionScalarType)):
        raise CustomMessageWiringError(
            f"shape_of_table_type{repr(tp)} "
            "should be an instance of HgTupleFixedScalarType or HgTupleCollectionScalarType"
        )
    # The tp is now an instance of HgTupleFixedScalarType or HgTupleCollectionScalarType
    if type(tp) is HgTupleCollectionScalarType:
        is_keyed = True
        tp = tuple(t.py_type for t in tp.element_type.element_types[3:])  # Skip dt, as_of, removed
    else:
        is_keyed = False
        tp = tuple(t.py_type for t in tp.element_types[2:])  # Skip dt, as_of

    if expect_keys is not None:
        if is_keyed != expect_keys:
            if is_keyed:
                raise CustomMessageWiringError(f"{repr(tp)} did not expect to have a keyed shape")
            else:
                raise CustomMessageWiringError(f"{repr(tp)} expected to have a keyed shape")

    if expect_length is not None:
        if len(tp) != expect_length:
            raise CustomMessageWiringError(f"{repr(tp)} expected have a length of {repr(expect_length)}")

    return tp, is_keyed
