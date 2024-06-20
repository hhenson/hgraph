from statistics import stdev, variance
from typing import Type

from hgraph._impl._types._ref import PythonTimeSeriesReference
from hgraph._impl._types._tss import PythonSetDelta
from hgraph._operators import sub_, getitem_, min_, max_, sum_, mean, var, str_, std, div_, bit_and, bit_or, bit_xor, \
    not_, eq_, len_, zero, add_, floordiv_, pow_, lshift_, rshift_, ne_, neg_, pos_, invert_, abs_, union, mul_, mod_, \
    all_
from hgraph._types._ref_type import REF
from hgraph._types._scalar_types import NUMBER, KEYABLE_SCALAR
from hgraph._types._time_series_types import TIME_SERIES_TYPE
from hgraph._types._ts_type import TS
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._tss_type import TSS, TSS_OUT
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._types._typing_utils import clone_typevar
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._reduce import reduce

__all__ = tuple()


@graph(overloads=len_)
def len_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
    from hgraph import const

    return const(_sz.SIZE)


@compute_node(overloads=getitem_, requires=lambda m, s: 0 <= s["index"] < m[SIZE])
def getitem_tsl_scalar(ts: REF[TSL[TIME_SERIES_TYPE, SIZE]], key: int) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSL referenced
    """
    if ts.value.valid:
        if ts.value.has_peer:
            return PythonTimeSeriesReference(ts.value.output[key])
        else:
            return ts.value.items[key]
    else:
        return PythonTimeSeriesReference()


@compute_node(overloads=getitem_)
def getitem_tsl_ts(
        ts: REF[TSL[TIME_SERIES_TYPE, SIZE]], key: TS[int], _sz: Type[SIZE] = AUTO_RESOLVE
) -> REF[TIME_SERIES_TYPE]:
    """
    Return a reference to an item in the TSL referenced
    """
    if key.value < 0 or key.value >= _sz.SIZE:
        return PythonTimeSeriesReference()

    if ts.value.valid:
        if ts.value.has_peer:
            return PythonTimeSeriesReference(ts.value.output[key.value])
        else:
            return ts.value.items[key.value]
    else:
        return PythonTimeSeriesReference()


@graph(overloads=sum_)
def sum_tsl_unary(tsl: TSL[TS[NUMBER], SIZE], tp: Type[TS[NUMBER]] = AUTO_RESOLVE) -> TS[NUMBER]:
    if len(tsl) == 1:
        return tsl[0]
    else:
        return _sum_tsl_unary(tsl, zero(tp, sum_))


@compute_node(overloads=sum_)
def _sum_tsl_unary(tsl: TSL[TS[NUMBER], SIZE], zero_ts: TS[NUMBER]) -> TS[NUMBER]:
    return sum((t.value for t in tsl.valid_values()), start=zero_ts.value)


SIZE_1 = clone_typevar(SIZE, "SIZE_1")


@graph(overloads=sum_)
def sum_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TIME_SERIES_TYPE, SIZE_1]:
    """
    Item-wise sum() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    if len(tsl) == 1:
        return tsl[0]
    else:
        return TSL.from_ts(*(sum_(*tsls) for tsls in tsl))


@graph(overloads=add_)
def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise addition of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a + b for a, b in zip(lhs, rhs)))


@graph(overloads=sub_)
def sub_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise subtraction of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a - b for a, b in zip(lhs, rhs)))


@graph(overloads=mul_)
def mul_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise product of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a * b for a, b in zip(lhs, rhs)))


@graph(overloads=div_)
def div_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise division of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a / b for a, b in zip(lhs, rhs)))


@graph(overloads=floordiv_)
def floordiv_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise floor division of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a // b for a, b in zip(lhs, rhs)))


@graph(overloads=mod_)
def mod_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise lhs % rhs of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a % b for a, b in zip(lhs, rhs)))


@graph(overloads=pow_)
def pow_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise lhs ** rhs of the elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a ** b for a, b in zip(lhs, rhs)))


@graph(overloads=lshift_)
def lshift_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise leftshift of the TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a << b for a, b in zip(lhs, rhs)))


@graph(overloads=rshift_)
def rshift_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise leftshift of the TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a >> b for a, b in zip(lhs, rhs)))


@graph(overloads=bit_and)
def bit_and_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise bitwise AND of the TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a & b for a, b in zip(lhs, rhs)))


@graph(overloads=bit_or)
def bit_or_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise bitwise OR of the TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a | b for a, b in zip(lhs, rhs)))


@graph(overloads=bit_xor)
def bit_xor_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise bitwise XOR of the TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a ^ b for a, b in zip(lhs, rhs)))


@graph(overloads=eq_)
def eq_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[bool]:
    """
    Equality operator for two TSLs.  Asymmetrical missing values cause a False return
    """
    return len(lhs) == len(rhs) and all_(*(eq_(a, b) for a, b in zip(lhs, rhs)))


@graph(overloads=ne_)
def ne_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TS[bool]:
    """
    Not Equality operator for two TSLs.  Asymmetrical missing values cause a True return.
    """
    return not_(eq_(lhs, rhs))


@graph(overloads=neg_)
def neg_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise unary negative of the TSL elements
    """
    return TSL.from_ts(*(neg_(a) for a in ts))


@graph(overloads=pos_)
def pos_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise unary positive of the TSL elements
    """
    return TSL.from_ts(*(pos_(a) for a in ts))


@graph(overloads=invert_)
def invert_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise unary invert of the TSL elements
    """
    return TSL.from_ts(*(invert_(a) for a in ts))


@graph(overloads=abs_)
def abs_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise abs() of the TSL elements
    """
    return TSL.from_ts(*(abs_(a) for a in ts))


@graph(overloads=min_)
def min_tsl_unary(tsl: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Minimum value in the TSL
    """
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return min_(tsl[0], tsl[1])
    else:
        return reduce(lambda a, b: min_(a, b), tsl, zero(tp, min_))


@graph(overloads=min_)
def min_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TIME_SERIES_TYPE, SIZE_1]:
    """
    Item-wise min() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    return TSL.from_ts(*(min_(*tsls) for tsls in tsl))


@graph(overloads=max_)
def max_tsl_unary(tsl: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Maximum value in the TSL
    """
    if len(tsl) == 1:
        return tsl[0]
    elif len(tsl) == 2:
        return max_(tsl[0], tsl[1])
    else:
        return reduce(lambda a, b: max_(a, b), tsl, zero(tp, max_))


@graph(overloads=max_)
def max_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TIME_SERIES_TYPE, SIZE_1]:
    """
    Item-wise max() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    return TSL.from_ts(*(max_(*tsls) for tsls in tsl))


@compute_node(overloads=str_)
def str_(ts: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[str]:
    return str(ts.value)


@compute_node(valid=tuple(), overloads=union)
def union_tsl_tss(*tsl: TSL[TSS[KEYABLE_SCALAR], SIZE], _output: TSS_OUT[KEYABLE_SCALAR] = None) -> TSS[KEYABLE_SCALAR]:
    tss: TSS[KEYABLE_SCALAR, SIZE]
    to_add: set[KEYABLE_SCALAR] = set()
    to_remove: set[KEYABLE_SCALAR] = set()
    for tss in tsl.modified_values():
        to_add |= tss.added()
        to_remove |= tss.removed()
    if disputed := to_add.intersection(to_remove):
        # These items are marked for addition and removal, so at least some set is hoping to add these items.
        # Thus, overall these are an add, unless they are already added.
        new_items = disputed.intersection(_output.value)
        to_remove -= new_items
    to_remove &= _output.value  # Only remove items that are already in the output.
    if to_remove:
        # Now we need to make sure there are no items that may be duplicated in other inputs.
        for tss in tsl.valid_values():
            to_remove -= to_remove.intersection(tss.value)  # Remove items that exist in an input
            if not to_remove:
                break
    return PythonSetDelta(to_add, to_remove)


@graph(overloads=mean)
def mean_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TS[float], SIZE_1]:
    """
    Item-wise mean() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    if len(tsl) == 1:
        return tsl[0]
    else:
        return TSL.from_ts(*(mean(*tsls) for tsls in tsl))


@graph(overloads=mean)
def mean_tsl_unary_number(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[float]:
    from hgraph import DivideByZero

    return div_(sum_(ts), _sz.SIZE, divide_by_zero=DivideByZero.NAN)


@graph(overloads=std)
def std_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TS[float], SIZE_1]:
    """
    Item-wise std() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    if len(tsl) == 1:
        return std(tsl[0])
    else:
        return TSL.from_ts(*(std(*tsls) for tsls in tsl))


@compute_node(overloads=std)
def std_tsl_unary_number(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[float]:
    valid_elements = tuple(t.value for t in ts if t.valid)
    n_valid = len(valid_elements)
    if n_valid <= 1:
        return 0.0
    else:
        return stdev(valid_elements)


@graph(overloads=var)
def var_tsl_multi(*tsl: TSL[TSL[TIME_SERIES_TYPE, SIZE], SIZE_1]) -> TSL[TS[float], SIZE_1]:
    """
    Item-wise std() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    if len(tsl) == 1:
        return var(tsl[0])
    else:
        return TSL.from_ts(*(var(*tsls) for tsls in tsl))


@compute_node(overloads=var)
def var_tsl_unary_number(ts: TSL[TS[NUMBER], SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[float]:
    valid_elements = tuple(t.value for t in ts if t.valid)
    n_valid = len(valid_elements)
    if n_valid <= 1:
        return 0.0
    else:
        return float(variance(valid_elements))
