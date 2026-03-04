import math

from hgraph import (
    add_,
    TS,
    compute_node,
    default,
    sub_,
    div_,
    NUMBER,
    mul_,
    floordiv_,
    mod_,
    divmod_,
    TSL,
    Size,
    pow_,
    eq_,
    lt_,
    gt_,
    le_,
    ge_,
    lshift_,
    rshift_,
    bit_and,
    bit_or,
    bit_xor,
    neg_,
    pos_,
    invert_,
    abs_,
    DivideByZero,
    ln,
)
from hgraph._types._scalar_types import NUMBER_2

__all__ = tuple()


@compute_node(overloads=add_)
def add_float_to_int(lhs: TS[int], rhs: TS[float]) -> TS[float]:
    """
    Adds a timeseries of float to a timeseries of int
    """
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def add_int_to_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    """
    Adds two int timeseries values.
    """
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def add_float_to_float(lhs: TS[float], rhs: TS[float]) -> TS[float]:
    """
    Adds two float timeseries values.
    """
    return lhs.value + rhs.value


@compute_node(overloads=add_)
def add_int_to_float(lhs: TS[float], rhs: TS[int]) -> TS[float]:
    """
    Adds a timeseries of int to a timeseries of float
    """
    return lhs.value + rhs.value


@compute_node(overloads=sub_)
def sub_int_from_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    """
    Subtracts one int timeseries from another.
    """
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_float_from_float(lhs: TS[float], rhs: TS[float]) -> TS[float]:
    """
    Subtracts one float timeseries from another.
    """
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_int_from_float(lhs: TS[float], rhs: TS[int]) -> TS[float]:
    """
    Subtracts a timeseries of int from a timeseries of float
    """
    return lhs.value - rhs.value


@compute_node(overloads=sub_)
def sub_float_from_int(lhs: TS[int], rhs: TS[float]) -> TS[float]:
    """
    Subtracts a timeseries of float from a timeseries of int
    """
    return lhs.value - rhs.value


@compute_node(overloads=mul_)
def mul_int_and_int(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    """
    Multiplies two int timeseries values.
    """
    return lhs.value * rhs.value


@compute_node(overloads=mul_)
def mul_float_and_float(lhs: TS[float], rhs: TS[float]) -> TS[float]:
    """
    Multiplies two float timeseries values.
    """
    return lhs.value * rhs.value


@compute_node(overloads=mul_)
def mul_float_and_int(lhs: TS[float], rhs: TS[int]) -> TS[float]:
    """
    Multiplies a timeseries of int with a timeseries of float
    """
    return lhs.value * rhs.value


@compute_node(overloads=mul_)
def mul_int_and_float(lhs: TS[int], rhs: TS[float]) -> TS[float]:
    """
    Multiplies a timeseries of float with a timeseries of int
    """
    return lhs.value * rhs.value


@compute_node(overloads=lshift_)
def lshift_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value << rhs.value


@compute_node(overloads=rshift_)
def rshift_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value >> rhs.value


@compute_node(overloads=bit_and)
def bit_and_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value & rhs.value


@compute_node(overloads=bit_or)
def bit_or_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value | rhs.value


@compute_node(overloads=bit_xor)
def bit_xor_ints(lhs: TS[int], rhs: TS[int]) -> TS[int]:
    return lhs.value ^ rhs.value


@compute_node(overloads=div_)
def div_numbers(lhs: TS[NUMBER], rhs: TS[NUMBER_2], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[float]:
    """
    Divides a numeric timeseries by another
    """
    try:
        return lhs.value / rhs.value
    except ZeroDivisionError:
        match divide_by_zero:
            case DivideByZero.NAN:
                return float("NaN")
            case DivideByZero.INF:
                return float("inf")
            case DivideByZero.NONE:
                return
            case DivideByZero.ZERO:
                return 0.0
            case DivideByZero.ONE:
                return 1.0
            case _:
                raise


@compute_node(overloads=floordiv_)
def floordiv_numbers(
    lhs: TS[NUMBER], rhs: TS[NUMBER_2], divide_by_zero: DivideByZero = DivideByZero.ERROR
) -> TS[float]:
    """
    Floor divides a numeric timeseries by another
    """
    try:
        return lhs.value // rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NAN:
            return float("NaN")
        elif divide_by_zero is DivideByZero.INF:
            return float("inf")
        elif divide_by_zero is DivideByZero.NONE:
            return
        elif divide_by_zero is DivideByZero.ZERO:
            return 0.0
        elif divide_by_zero is DivideByZero.ONE:
            return 1.0
        else:
            raise


@compute_node(overloads=floordiv_)
def floordiv_ints(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[int]:
    """
    Floor divides an int timeseries by another
    """
    try:
        return lhs.value // rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NONE:
            return
        elif divide_by_zero is DivideByZero.ZERO:
            return 0
        elif divide_by_zero is DivideByZero.ONE:
            return 1.0
        else:
            raise


@compute_node(overloads=mod_)
def mod_numbers(lhs: TS[NUMBER], rhs: TS[NUMBER_2], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[float]:
    """
    Modulo a numeric timeseries by another
    """
    try:
        return lhs.value % rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NAN:
            return float("NaN")
        elif divide_by_zero is DivideByZero.INF:
            return float("inf")
        elif divide_by_zero is DivideByZero.NONE:
            return
        else:
            raise


@compute_node(overloads=mod_)
def mod_ints(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[int]:
    """
    Floor divides a int timeseries by another
    """
    try:
        return lhs.value % rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NONE:
            return
        else:
            raise


@compute_node(overloads=divmod_)
def divmod_numbers(
    lhs: TS[NUMBER], rhs: TS[NUMBER_2], divide_by_zero: DivideByZero = DivideByZero.ERROR
) -> TSL[TS[float], Size[2]]:
    try:
        return divmod(lhs.value, rhs.value)
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NAN:
            return float("NaN")
        elif divide_by_zero is DivideByZero.INF:
            return float("inf")
        elif divide_by_zero is DivideByZero.NONE:
            return
        elif divide_by_zero is DivideByZero.ZERO:
            return 0.0
        elif divide_by_zero is DivideByZero.ONE:
            return 1.0
        else:
            raise


@compute_node(overloads=divmod_)
def divmod_ints(lhs: TS[int], rhs: TS[int], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TSL[TS[int], Size[2]]:
    try:
        return divmod(lhs.value, rhs.value)
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NONE:
            return
        else:
            raise


@compute_node(overloads=pow_)
def pow_int_float(lhs: TS[int], rhs: TS[float], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[float]:
    """
    Raises an int time-series value to the power of a float time-series value
    """
    try:
        return lhs.value**rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NAN:
            return float("NaN")
        elif divide_by_zero is DivideByZero.INF:
            return float("inf")
        elif divide_by_zero is DivideByZero.NONE:
            return
        elif divide_by_zero is DivideByZero.ZERO:
            return 0.0
        elif divide_by_zero is DivideByZero.ONE:
            return 1.0
        else:
            raise


@compute_node(overloads=pow_)
def pow_float_int(lhs: TS[float], rhs: TS[int], divide_by_zero: DivideByZero = DivideByZero.ERROR) -> TS[float]:
    """
    Raises a float time-series value to the power of an int time-series value
    """
    try:
        return lhs.value**rhs.value
    except ZeroDivisionError:
        if divide_by_zero is DivideByZero.NAN:
            return float("NaN")
        elif divide_by_zero is DivideByZero.INF:
            return float("inf")
        elif divide_by_zero is DivideByZero.NONE:
            return
        elif divide_by_zero is DivideByZero.ZERO:
            return 0.0
        elif divide_by_zero is DivideByZero.ONE:
            return 1.0
        else:
            raise


EPSILON = 1e-10


@compute_node(overloads=eq_)
def eq_int_int(lhs: TS[int], rhs: TS[int]) -> TS[bool]:
    """
    Test for equality of two int numbers.
    """
    return bool(lhs.value == rhs.value)


@compute_node(overloads=eq_)
def eq_float_int(lhs: TS[float], rhs: TS[int], epsilon: TS[float] = EPSILON) -> TS[bool]:
    """
    Test for approximate equality of a float and int number
    """
    epsilon = epsilon.value
    return bool(-epsilon <= rhs.value - lhs.value <= epsilon)


@compute_node(overloads=eq_)
def eq_int_float(lhs: TS[int], rhs: TS[float], epsilon: TS[float] = EPSILON) -> TS[bool]:
    """
    Test for approximate equality of an int and float number
    """
    epsilon = epsilon.value
    return bool(-epsilon <= rhs.value - lhs.value <= epsilon)


@compute_node(overloads=eq_)
def eq_float_float(lhs: TS[float], rhs: TS[float], epsilon: TS[float] = EPSILON) -> TS[bool]:
    """
    Test for approximate equality of two float numbers
    """
    epsilon = epsilon.value
    return bool(-epsilon <= rhs.value - lhs.value <= epsilon)


@compute_node(overloads=lt_)
def lt_int_int(lhs: TS[int], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@compute_node(overloads=lt_)
def lt_int_float(lhs: TS[int], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@compute_node(overloads=lt_)
def lt_float_int(lhs: TS[float], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@compute_node(overloads=lt_)
def lt_float_float(lhs: TS[float], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value < rhs.value)


@compute_node(overloads=le_)
def le_int_int(lhs: TS[int], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=le_)
def le_int_float(lhs: TS[int], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=le_)
def le_float_int(lhs: TS[float], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=le_)
def le_float_float(lhs: TS[float], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value <= rhs.value)


@compute_node(overloads=gt_)
def gt_int_int(lhs: TS[int], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value > rhs.value)


@compute_node(overloads=gt_)
def gt_int_float(lhs: TS[int], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value > rhs.value)


@compute_node(overloads=gt_)
def gt_float_int(lhs: TS[float], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value > rhs.value)


@compute_node(overloads=gt_)
def gt_float_float(lhs: TS[float], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value > rhs.value)


@compute_node(overloads=ge_)
def ge_int_int(lhs: TS[int], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value >= rhs.value)


@compute_node(overloads=ge_)
def ge_int_float(lhs: TS[int], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value >= rhs.value)


@compute_node(overloads=ge_)
def ge_float_int(lhs: TS[float], rhs: TS[int]) -> TS[bool]:
    return bool(lhs.value >= rhs.value)


@compute_node(overloads=ge_)
def ge_float_float(lhs: TS[float], rhs: TS[float]) -> TS[bool]:
    return bool(lhs.value >= rhs.value)


@compute_node(overloads=neg_)
def neg_int(ts: TS[int]) -> TS[int]:
    return -ts.value


@compute_node(overloads=neg_)
def neg_float(ts: TS[float]) -> TS[float]:
    return -ts.value


@compute_node(overloads=pos_)
def pos_int(ts: TS[int]) -> TS[int]:
    return +ts.value


@compute_node(overloads=pos_)
def pos_float(ts: TS[float]) -> TS[float]:
    return +ts.value


@compute_node(overloads=invert_)
def invert_int(ts: TS[int]) -> TS[int]:
    return ~ts.value


@compute_node(overloads=abs_)
def abs_int(ts: TS[int]) -> TS[int]:
    return abs(ts.value)


@compute_node(overloads=abs_)
def abs_float(ts: TS[float]) -> TS[float]:
    return abs(ts.value)


@compute_node(overloads=ln)
def ln_impl(ts: TS[float]) -> TS[float]:
    return math.log(ts.value)
