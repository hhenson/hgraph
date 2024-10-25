from hgraph import (
    compute_node,
    getitem_,
    TS,
    Series,
    SCALAR,
    min_,
    max_,
    div_,
    sub_,
    NUMBER,
    NUMBER_2,
    add_,
    mul_,
    graph,
    cast_,
    contains_,
)

__all__ = tuple()


@compute_node(overloads=getitem_)
def get_item_series(series: TS[Series[SCALAR]], key: int) -> TS[SCALAR]:
    return series.value[key]


@compute_node(overloads=getitem_)
def get_item_series_ts(series: TS[Series[SCALAR]], key: TS[int]) -> TS[SCALAR]:
    return series.value[key.value]


@compute_node(overloads=min_)
def min_series(series: TS[Series[SCALAR]]) -> TS[SCALAR]:
    return series.value.min()


@compute_node(overloads=max_)
def max_series(series: TS[Series[SCALAR]]) -> TS[SCALAR]:
    return series.value.max()


@compute_node(overloads=div_)
def div_series_series(lhs: TS[Series[NUMBER]], rhs: TS[Series[NUMBER_2]]) -> TS[Series[float]]:
    return lhs.value / rhs.value


@compute_node(overloads=div_)
def div_series_number(lhs: TS[Series[NUMBER]], rhs: TS[NUMBER_2]) -> TS[Series[float]]:
    return lhs.value / rhs.value


@compute_node(overloads=mul_)
def mul_series_float_series_float(lhs: TS[Series[NUMBER]], rhs: TS[Series[NUMBER_2]]) -> TS[Series[NUMBER_2]]:
    return lhs.value * rhs.value


@compute_node(overloads=mul_)
def mul_series_float_series_int(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
    return lhs.value * rhs.value


@compute_node(overloads=mul_)
def mul_series_int_float(lhs: TS[Series[NUMBER]], rhs: TS[NUMBER_2]) -> TS[Series[NUMBER_2]]:
    return lhs.value * rhs.value


@graph(overloads=mul_)
def mul_series_float_int(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
    return lhs * cast_(float, rhs)


@compute_node(overloads=sub_)
def sub_series_float_float(lhs: TS[Series[NUMBER]], rhs: TS[NUMBER_2]) -> TS[Series[NUMBER_2]]:
    return lhs.value - rhs.value


@graph(overloads=sub_)
def sub_series_float_int(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
    return lhs - cast_(float, rhs)


@compute_node(overloads=sub_)
def sub_series_int_series_int(lhs: TS[Series[NUMBER]], rhs: TS[Series[NUMBER_2]]) -> TS[Series[NUMBER_2]]:
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_series_float_series_int(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
    return lhs.value - rhs.value


@compute_node(overloads=add_)
def add_series_float_float(lhs: TS[Series[NUMBER]], rhs: TS[NUMBER_2]) -> TS[Series[NUMBER_2]]:
    return lhs.value + rhs.value


@graph(overloads=add_)
def add_series_float_int(lhs: TS[Series[float]], rhs: TS[int]) -> TS[Series[float]]:
    return lhs + cast_(float, rhs)


@compute_node(overloads=add_)
def add_series_int_series_int(lhs: TS[Series[NUMBER]], rhs: TS[Series[NUMBER_2]]) -> TS[Series[NUMBER_2]]:
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def add_series_float_series_int(lhs: TS[Series[float]], rhs: TS[Series[int]]) -> TS[Series[float]]:
    return lhs.value + rhs.value


@compute_node(overloads=contains_)
def contains_series(series: TS[Series[SCALAR]], value: TS[SCALAR]) -> TS[bool]:
    return value.value in series.value