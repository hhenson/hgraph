from statistics import stdev, variance
from typing import Type, Mapping

import numpy as np

from hgraph import (
    SCALAR,
    TS,
    compute_node,
    add_,
    sub_,
    mul_,
    pow_,
    lshift_,
    rshift_,
    bit_and,
    bit_or,
    bit_xor,
    eq_,
    ne_,
    lt_,
    le_,
    gt_,
    ge_,
    neg_,
    pos_,
    invert_,
    abs_,
    len_,
    and_,
    or_,
    not_,
    contains_,
    SCALAR_1,
    min_,
    max_,
    graph,
    TS_OUT,
    sum_,
    TSL,
    SIZE,
    AUTO_RESOLVE,
    zero,
    mean,
    std,
    var,
    cmp_,
    CmpResult,
    TSW,
    WINDOW_SIZE,
    WINDOW_SIZE_MIN,
    NUMBER,
)

__all__ = tuple()


@compute_node(overloads=add_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__add__"))
def add_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Adds two timeseries values of scalars (which support +)
    """
    return lhs.value + rhs.value


@compute_node(overloads=sub_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__sub__"))
def sub_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Subtracts two timeseries values of scalars (which support -)
    """
    return lhs.value - rhs.value


@compute_node(overloads=mul_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__mul__"))
def mul_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Multiples two timeseries values of scalars (which support *)
    """
    return lhs.value * rhs.value


@compute_node(overloads=pow_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__pow__"))
def pow_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Raises a timeseries value to the power of the other timeseries value
    """
    return lhs.value**rhs.value


@compute_node(overloads=lshift_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__lshift__"))
def lshift_scalars(lhs: TS[SCALAR], rhs: TS[int]) -> TS[SCALAR]:
    """
    Shifts the values in the lhs timeseries left by the rhs value
    """
    return lhs.value << rhs.value


@compute_node(overloads=rshift_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__rshift__"))
def rshift_scalars(lhs: TS[SCALAR], rhs: TS[int]) -> TS[SCALAR]:
    """
    Shifts the values in the lhs timeseries right by the rhs value
    """
    return lhs.value >> rhs.value


@compute_node(overloads=bit_and, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__and__"))
def bit_and_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Timeseries equivalent of lhs & rhs
    """
    return lhs.value & rhs.value


@compute_node(overloads=bit_or, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__or__"))
def bit_or_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Timeseries equivalent of lhs | rhs
    """
    return lhs.value | rhs.value


@compute_node(overloads=bit_xor, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__xor__"))
def bit_xor_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Timeseries equivalent of lhs ^ rhs
    """
    return lhs.value ^ rhs.value


@compute_node(overloads=eq_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__eq__"))
def eq_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Equality of two scalar timeseries
    """
    return bool(lhs.value == rhs.value)


@compute_node(
    overloads=cmp_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__eq__") and hasattr(m[SCALAR].py_type, "__lt__")
)
def cmp_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR_1]) -> TS[CmpResult]:
    """
    Cmp of two scalar timeseries
    """
    v1 = lhs.value
    v2 = rhs.value
    return CmpResult.EQ if v1 == v2 else CmpResult.LT if v1 < v2 else CmpResult.GT


@compute_node(overloads=cmp_)
def cmp_ts_dict(lhs: TS[Mapping[SCALAR, SCALAR_1]], rhs: TS[Mapping[SCALAR, SCALAR_1]]) -> TS[CmpResult]:
    v1 = lhs.value
    v2 = rhs.value
    l1 = len(v1)
    l2 = len(v2)
    if l1 < l2:
        return CmpResult.LT
    elif l1 > l2:
        return CmpResult.GT
    else:
        if (s1 := sorted(v1.keys())) == (s2 := sorted(v2.keys())):
            for k in s1:
                t1 = v1[k]
                t2 = v2[k]
                if t1 < t2:
                    return CmpResult.LT
                elif t1 != t2:
                    return CmpResult.GT
            return CmpResult.EQ
        else:
            if s1 < s2:
                return CmpResult.LT
            else:
                return CmpResult.GT


@compute_node(overloads=ne_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__ne__"))
def ne_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Not equality of two scalar timeseries
    """
    return bool(lhs.value != rhs.value)


@compute_node(overloads=lt_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__lt__"))
def lt_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Test for less than of two scalar timeseries
    """
    return bool(lhs.value < rhs.value)


@compute_node(overloads=le_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__le__"))
def le_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Test for less than or equal of two scalar timeseries
    """
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=gt_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__gt__"))
def gt_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Test for greater than of two scalar timeseries
    """
    return bool(lhs.value > rhs.value)


@compute_node(overloads=ge_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__ge__"))
def ge_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Test for greater than or equal of two scalar timeseries
    """
    return bool(lhs.value >= rhs.value)


@compute_node(overloads=neg_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__neg__"))
def neg_scalar(ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Unary negative operator for scalar timeseries
    """
    return -ts.value


@compute_node(overloads=pos_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__pos__"))
def pos_scalar(ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Unary positive operator for scalar timeseries
    """
    return +ts.value


@compute_node(overloads=invert_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__invert__"))
def invert_scalar(ts: TS[SCALAR]) -> TS[int]:
    """
    Unary ~ operator for scalar timeseries
    """
    return ~ts.value


@compute_node(overloads=abs_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__abs__"))
def abs_scalar(ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Unary abs() operator for scalar timeseries
    """
    return abs(ts.value)


@compute_node(
    overloads=len_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__len__") or m[SCALAR].py_type.__name__ == "Frame"
)
def len_scalar(ts: TS[SCALAR]) -> TS[int]:
    """
    The length of the value of the timeseries
    """
    return len(ts.value)


@compute_node(overloads=not_)
def not_scalar(ts: TS[SCALAR]) -> TS[bool]:
    """
    Unary ``not``
    Returns True or False according to the (inverse of the) 'truthiness' of the timeseries value
    """
    return not ts.value


@compute_node(overloads=and_)
def and_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Binary AND
    """
    return bool(lhs.value and rhs.value)


@compute_node(overloads=or_)
def or_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[bool]:
    """
    Binary OR
    """
    return bool(lhs.value or rhs.value)


@compute_node(overloads=contains_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__contains__"))
def contains_scalar(ts: TS[SCALAR], key: TS[SCALAR_1]) -> TS[bool]:
    """
    Implements the python ``in`` operator
    """
    return ts.value.__contains__(key.value)


@graph(overloads=min_)
def min_scalar(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True) -> TS[SCALAR]:
    if len(ts) == 1:
        return min_scalar_unary(ts[0])
    elif len(ts) == 2:
        return min_scalar_binary(ts[0], ts[1], __strict__)
    else:
        return min_scalar_multi(*ts, default_value=default_value, __strict__=__strict__)


@compute_node(valid=lambda m, s: ("lhs", "rhs") if s["__strict__"] else ())
def min_scalar_binary(lhs: TS[SCALAR], rhs: TS[SCALAR], __strict__: bool = True) -> TS[SCALAR]:
    """
    Binary min()
    """
    if lhs.valid and rhs.valid:
        return min(lhs.value, rhs.value)
    if lhs.valid:
        return lhs.value
    if rhs.valid:
        return rhs.value


@compute_node
def min_scalar_unary(ts: TS[SCALAR], _output: TS_OUT[SCALAR] = None) -> TS[SCALAR]:
    """
    Unary min()
    The default implementation (here) is a running min
    Unary min for scalar collections return the min of the current collection value.
    These are overloaded separately
    """
    if not _output.valid:
        return ts.value
    elif ts.value < _output.value:
        return ts.value


@compute_node(all_valid=lambda m, s: ("ts",) if s["__strict__"] else None)
def min_scalar_multi(
    *ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True
) -> TS[SCALAR]:
    """
    Multi-arg min()
    """
    return min((arg.value for arg in ts if arg.valid), default=default_value.value)


@graph(overloads=max_)
def max_scalar(*ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True) -> TS[SCALAR]:
    if len(ts) == 1:
        return max_scalar_unary(ts[0])
    elif len(ts) == 2:
        return max_scalar_binary(ts[0], ts[1], __strict__)
    else:
        return max_scalar_multi(*ts, default_value=default_value, __strict__=__strict__)


@compute_node
def max_scalar_unary(ts: TS[SCALAR], _output: TS_OUT[SCALAR] = None) -> TS[SCALAR]:
    """
    Unary max()
    The default implementation (here) is a running max
    Unary max for scalar collections return the max of the current collection value.
    These are overloaded separately
    """
    if not _output.valid:
        return ts.value
    elif ts.value > _output.value:
        return ts.value


@compute_node(valid=lambda m, s: ("lhs", "rhs") if s["__strict__"] else ())
def max_scalar_binary(lhs: TS[SCALAR], rhs: TS[SCALAR], __strict__: bool = True) -> TS[SCALAR]:
    """
    Binary max()
    """
    if lhs.valid and rhs.valid:
        return max(lhs.value, rhs.value)
    if lhs.valid:
        return lhs.value
    if rhs.valid:
        return rhs.value


@compute_node(all_valid=lambda m, s: ("ts",) if s["__strict__"] else None)
def max_scalar_multi(
    *ts: TSL[TS[SCALAR], SIZE], default_value: TS[SCALAR] = None, __strict__: bool = True
) -> TS[SCALAR]:
    """
    Multi-arg max()
    """
    return max((arg.value for arg in ts if arg.valid), default=default_value.value)


@graph(overloads=sum_)
def sum_scalars(*ts: TSL[TS[SCALAR], SIZE], tp: Type[TS[SCALAR]] = AUTO_RESOLVE) -> TS[SCALAR]:
    if len(ts) == 1:
        return sum_scalar_unary(ts[0])
    elif len(ts) == 2:
        return sum_scalars_binary(ts[0], ts[1], zero_value=zero(tp, sum_))
    else:
        return sum_scalars_multi(*ts, zero_value=zero(tp, sum_))


@compute_node
def sum_scalar_unary(ts: TS[SCALAR], _output: TS_OUT[SCALAR] = None) -> TS[SCALAR]:
    """
    Unary sum()
    The default implementation (here) is a running sum
    Unary sum for scalar collections return the sum of the current collection value.
    These are overloaded separately
    """
    if not _output.valid:
        return ts.value
    else:
        return ts.value + _output.value


@graph
def sum_scalars_binary(lhs: TS[SCALAR], rhs: TS[SCALAR], zero_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Binary sum (i.e. addition) with default
    """
    from hgraph import default

    return default(lhs + rhs, zero_value)


@compute_node
def sum_scalars_multi(*ts: TSL[TS[SCALAR], SIZE], zero_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Multi-arg sum()
    """
    return sum((arg.value for arg in ts), start=zero_value.value)


@graph(overloads=mean)
def mean_scalars(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return mean_scalar_unary(ts[0])
    elif len(ts) == 2:
        return mean_scalars_binary(ts[0], ts[1])
    else:
        return mean_scalars_multi(*ts)


@graph
def mean_scalar_unary(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[float]:
    """
    Unary mean()
    The default implementation (here) is a running mean
    Unary mean for scalar collections return the mean of the current collection value.
    These are overloaded separately
    """
    from hgraph import cast_, count

    if tp is float:
        return sum_(ts) / count(ts)
    else:
        return cast_(float, sum_(ts)) / count(ts)


@compute_node(overloads=mean, all_valid=("ts",))
def mean(ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN]) -> TS[float]:
    """Computes the mean of a time series window"""
    return np.mean(ts.value)


@graph
def mean_scalars_binary(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[float]:
    """
    Binary mean
    """
    from hgraph import default

    return default((lhs + rhs) / 2.0, float("NaN"))


@compute_node
def mean_scalars_multi(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    """
    Multi-arg mean()
    """
    valid_elements = tuple(arg.value for arg in ts if arg.valid)
    n_valid = len(valid_elements)
    if n_valid == 0:
        return float("NaN")
    elif n_valid == 1:
        return float(valid_elements[0])
    else:
        return float(sum(valid_elements) / n_valid)


@graph(overloads=std)
def std_scalars(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return std_scalar_unary(ts[0])
    else:
        return std_scalars_multi(*ts)


@graph
def std_scalar_unary(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[float]:
    """
    Unary std()
    The default implementation (here) is a running std
    Unary std for scalar collections return the std of the current collection value.
    These are overloaded separately
    """
    from hgraph import count

    # TODO - this is a naive implementation.  See Welford's algorithm
    count_x = count(ts)
    sum_x = sum_(ts)
    mean_x = sum_x / count_x
    sum_x2 = sum_(ts * ts)
    return pow_((sum_x2 / count_x - mean_x * mean_x), 0.5)


@compute_node(overloads=std, all_valid=("ts",))
def std_tsw(ts: TSW[NUMBER, WINDOW_SIZE, WINDOW_SIZE_MIN], ddof: int = 0) -> TS[float]:
    """
    Computes the standard deviation of a time series window.
    This uses the numpy std function.

    :param ddof: int, optional. Means Delta Degrees of Freedom.  The divisor used in calculations
        is ``N - ddof``, where ``N`` represents the number of elements.
        By default, `ddof` is zero. See Notes for details about use of `ddof`.
    """
    return np.std(ts.value, ddof=ddof)


@compute_node
def std_scalars_multi(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    """
    Multi-arg std()
    """
    valid_elements = tuple(arg.value for arg in ts if arg.valid)
    if len(valid_elements) <= 1:
        return 0.0
    else:
        return float(stdev(valid_elements))


@graph(overloads=var)
def var_scalars(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return var_scalar_unary(ts[0])
    else:
        return var_scalars_multi(*ts)


@graph
def var_scalar_unary(ts: TS[SCALAR], tp: Type[SCALAR] = AUTO_RESOLVE) -> TS[float]:
    """
    Unary variance
    The default implementation (here) is a running variance
    Unary variance for scalar collections return the variance of the current collection value.
    These are overloaded separately
    """
    from hgraph.nodes import count

    # TODO - this is a naive implementation.  See Welford's algorithm
    count_x = count(ts)
    sum_x = sum_(ts)
    mean_x = sum_x / count_x
    sum_x2 = sum_(ts * ts)
    return sum_x2 / count_x - mean_x * mean_x


@compute_node
def var_scalars_multi(*ts: TSL[TS[SCALAR], SIZE]) -> TS[float]:
    """
    Multi-arg variance
    """
    valid_elements = tuple(arg.value for arg in ts if arg.valid)
    if len(valid_elements) <= 1:
        return 0.0
    else:
        return float(variance(valid_elements))
