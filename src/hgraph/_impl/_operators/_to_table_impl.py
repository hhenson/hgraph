from hgraph._impl._operators._to_table_dispatch_impl import extract_table_schema_raw_type, PartialSchema
from hgraph._operators import to_table, get_as_of, table_schema, TableSchema, make_table_schema, from_table
from hgraph._operators._to_table import ToTableMode, from_table_const, TABLE, table_shape
from hgraph._runtime import EvaluationClock
from hgraph._types import TS, TIME_SERIES_TYPE, STATE, AUTO_RESOLVE, OUT
from hgraph._wiring._decorators import compute_node, const_fn

__all__ = []


def _validate_not_type(tp, not_type) -> type:
    if isinstance(tp, not_type):
        raise RuntimeError(f"'{tp}' is not of type '{not_type}'")
    return tp


def _validate_is_type(tp, is_type) -> type:
    if not isinstance(tp, is_type):
        raise RuntimeError(f"'{tp}' is of type '{is_type}'")
    return tp


# Generic TimeSeries flattener


@const_fn(overloads=table_schema)
def table_schema_generic(tp: type[TIME_SERIES_TYPE]) -> TS[TableSchema]:
    partial_schema = extract_table_schema_raw_type(tp)
    return make_table_schema(
        tp=tp,
        keys=partial_schema.keys,
        types=partial_schema.types,
        partition_keys=partial_schema.partition_keys,
        removed_keys=partial_schema.remove_partition_keys,
    )


@compute_node(overloads=to_table, resolvers={TABLE: lambda m, s: table_shape(m[TIME_SERIES_TYPE].py_type)})
def to_table_generic(
    ts: TIME_SERIES_TYPE,
    mode: TS[ToTableMode] = ToTableMode.Tick,
    _clock: EvaluationClock = None,
    _tp: type[TIME_SERIES_TYPE] = AUTO_RESOLVE,
    _state: STATE = None,
) -> TS[TABLE]:
    schema: PartialSchema = _state.partial_schema

    if mode.value == ToTableMode.Tick:
        fn = schema.to_table
    elif mode.value == ToTableMode.Sample:
        fn = schema.to_table_sample
    elif mode.value == ToTableMode.Snap:
        fn = schema.to_table_snap

    if schema.partition_keys:
        return tuple(((ts.last_modified_time, get_as_of(_clock)) + i) for i in fn(ts))
    else:
        return (ts.last_modified_time, get_as_of(_clock)) + fn(ts)


@to_table_generic.start
def to_table_generic_start(_tp: type[TIME_SERIES_TYPE], _state: STATE):
    _state.partial_schema = extract_table_schema_raw_type(_tp)


@compute_node(overloads=from_table)
def from_table_generic(ts: TS[TABLE], _tp: type[OUT] = AUTO_RESOLVE, _state: STATE = None) -> OUT:
    schema: PartialSchema = _state.partial_schema
    # We strip out the time aspect as this would have been used to prepare the inputs, we only need to
    # process the main data bundle
    if schema.partition_keys:
        return schema.from_table(iter(value[2:] for value in ts.value))
    else:
        return schema.from_table(iter(ts.value[2:]))


@from_table_generic.start
def from_table_generic_start(_tp: type[OUT], _state: STATE):
    _state.partial_schema = extract_table_schema_raw_type(_tp)


@const_fn(overloads=from_table_const)
def from_table_const_generic(value: TABLE, _tp: type[OUT] = AUTO_RESOLVE) -> OUT:
    schema: PartialSchema = extract_table_schema_raw_type(_tp)
    # We strip out the time aspect as this would have been used to prepare the inputs, we only need to
    # process the main data bundle
    return schema.from_table(iter(value[2:]))
