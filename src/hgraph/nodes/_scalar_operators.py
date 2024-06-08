from hgraph import SCALAR, TS, compute_node, add_, sub_, mul_, pow_, lshift_, rshift_, bit_and, bit_or, bit_xor, eq_, \
    ne_, lt_, le_, gt_, ge_, neg_, pos_, invert_, abs_, len_, and_, or_, not_, contains_, SCALAR_1, min_, max_, graph, \
    TS_OUT, sum_, str_


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
    return lhs.value ** rhs.value


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


@compute_node(overloads=len_, requires=lambda m, s: hasattr(m[SCALAR].py_type, "__len__"))
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


@compute_node(overloads=min_)
def min_scalar(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Binary min()
    """
    return min(lhs.value, rhs.value)


@compute_node(overloads=min_)
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


@compute_node(overloads=max_)
def max_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Binary max()
    """
    return max(lhs.value, rhs.value)


@compute_node(overloads=max_)
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


@graph(overloads=sum_)
def sum_scalars(lhs: TS[SCALAR], rhs: TS[SCALAR]) -> TS[SCALAR]:
    """
    Binary sum (i.e. addition)
    """
    return lhs + rhs


@compute_node(overloads=sum_)
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


@compute_node(overloads=str_)
def str_scalar(ts: TS[SCALAR]) -> TS[str]:
    """
    Implements python str() for scalars
    """
    return str(ts.value)
