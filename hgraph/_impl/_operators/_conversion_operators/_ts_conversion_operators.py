from datetime import datetime, date

from hgraph import compute_node, TS, SCALAR, SCALAR_1, graph, AUTO_RESOLVE, OUT
from hgraph._operators._time_series_conversion import convert
from hgraph._types._scalar_types import DEFAULT

__all__ = ()


def _conversion_mode(from_scalar, to_scalar):
    if isinstance(from_scalar, type) and isinstance(to_scalar, type):
        if issubclass(from_scalar, to_scalar):  # upcasting
            if to_scalar is int and from_scalar is bool:  # python specific
                return 0
            return 1
        elif issubclass(to_scalar, from_scalar):  # downcasting
            if to_scalar is bool and from_scalar is int:  # python specific
                return 0
            return -1
        else:
            return 0  # unrelated types, use constructor

    if to_scalar in (bool, str):  # convert anything to those two
        return 0

    return False  # not supported by generic scalar conversion


def _check_conversion_mode(check_mode, from_scalar, to_scalar):
    if (mode := _conversion_mode(from_scalar, to_scalar)) is False:
        return f"{from_scalar} -> {to_scalar} is not supported by generic scalar conversion"
    return mode == check_mode


@compute_node(
    overloads=convert,
    requires=lambda m: m[SCALAR] != m[SCALAR_1]
    and _check_conversion_mode(0, m[SCALAR].py_type, m[SCALAR_1].py_type),
)
def convert_ts_scalar(
    ts: TS[SCALAR], to: type[TS[SCALAR_1]] = DEFAULT[OUT], s1_type: type[SCALAR_1] = AUTO_RESOLVE
) -> TS[SCALAR_1]:
    return s1_type(ts.value)


@compute_node(overloads=convert)
def convert_ts_str_to_bytes(ts: TS[str], to: type[TS[bytes]] = DEFAULT[OUT]) -> TS[bytes]:
    return ts.value.encode()


@compute_node(overloads=convert)
def convert_ts_bytes_to_str(ts: TS[bytes], to: type[TS[str]] = DEFAULT[OUT]) -> TS[str]:
    return ts.value.decode()


@graph(
    overloads=convert,
    requires=lambda m: m[SCALAR] != m[SCALAR_1]
    and _check_conversion_mode(1, m[SCALAR].py_type, m[SCALAR_1].py_type),
)
def convert_ts_scalar_upcast(
    ts: TS[SCALAR], to: type[TS[SCALAR_1]] = DEFAULT[OUT], s1_type: type[SCALAR_1] = AUTO_RESOLVE
) -> TS[SCALAR_1]:
    return ts  # upcasting is implicit in the graph


@compute_node(
    overloads=convert,
    requires=lambda m: m[SCALAR] != m[SCALAR_1]
    and _check_conversion_mode(-1, m[SCALAR].py_type, m[SCALAR_1].py_type),
)
def convert_ts_scalar_downcast(
    ts: TS[SCALAR], to: type[TS[SCALAR_1]] = DEFAULT[OUT], s1_type: type[SCALAR_1] = AUTO_RESOLVE
) -> TS[SCALAR_1]:
    ts = ts.value
    if isinstance(ts, s1_type):
        return ts
    elif ts.__class__ is date and s1_type is datetime:
        return datetime(ts.year, ts.month, ts.day)
    else:
        raise ValueError(
            f"Scalar downcast conversion failed: {ts} of type {ts.__class__.__name__} is not an instance of {s1_type}"
        )
