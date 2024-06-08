from typing import Type

from hgraph import compute_node, TSL, TIME_SERIES_TYPE, SIZE, SCALAR, TS, graph, AUTO_RESOLVE, NUMBER, REF, TSD, \
    union_tsl, TSS, KEYABLE_SCALAR, TSS_OUT, PythonSetDelta, add_, sub_, mul_, div_, floordiv_, mod_, pow_, lshift_, \
    rshift_, bit_and, bit_or, bit_xor, eq_, ne_, not_, neg_, pos_, invert_, abs_, min_, max_, reduce, zero, \
    str_, PythonTimeSeriesReference, len_, operator, sum_, getitem_
from hgraph._operators._control import merge, all_
from hgraph.nodes import const

__all__ = ("flatten_tsl_values", "tsl_to_tsd", "index_of")


@compute_node
def flatten_tsl_values(tsl: TSL[TIME_SERIES_TYPE, SIZE], all_valid: bool = False) -> TS[tuple[SCALAR, ...]]:
    """
    This will convert the TSL into a time-series of tuples. The value will be the value type of the time-series
    provided to the TSL. If the value has not ticked yet, the value will be None.
    The output type must be defined by the user.

    Usage:
    ```Python
        tsl: TSL[TS[float], Size[3]] = ...
        out = flatten_tsl[SCALAR: float](tsl)
    ```
    """
    return tsl.value if not all_valid or tsl.all_valid else None


@compute_node(overloads=merge)
def merge_default(*tsl: TSL[TIME_SERIES_TYPE, SIZE]) -> TIME_SERIES_TYPE:
    """
    Selects and returns the first of the values that tick (are modified) in the list provided.
    If more than one input is modified in the engine-cycle, it will return the first one that ticked in order of the
    list.
    """
    return next(tsl.modified_values()).delta_value


@graph(overloads=len_)
def len_tsl(ts: TSL[TIME_SERIES_TYPE, SIZE], _sz: type[SIZE] = AUTO_RESOLVE) -> TS[int]:
    return const(_sz.SIZE)


@graph(overloads=sum_)
def sum_tsl_unary(ts: TSL[TS[NUMBER], SIZE], tp: Type[TS[NUMBER]] = AUTO_RESOLVE) -> TS[NUMBER]:
    return _sum_tsl_unary(ts, zero(tp, sum_))


@compute_node(overloads=sum_)
def _sum_tsl_unary(ts: TSL[TS[NUMBER], SIZE], zero_ts: TS[NUMBER]) -> TS[NUMBER]:
    return sum((t.value for t in ts.valid_values()), start=zero_ts.value)


@compute_node
def tsl_to_tsd(tsl: TSL[REF[TIME_SERIES_TYPE], SIZE], keys: tuple[str, ...]) -> TSD[str, REF[TIME_SERIES_TYPE]]:
    """
    Converts a time series into a time series dictionary with the keys provided.
    """
    return {k: ts.value for k, ts in zip(keys, tsl) if ts.modified}


@compute_node(overloads=getitem_, requires=lambda m, s: 0 <= s['index'] < m[SIZE])
def tsl_get_item_default(ts: REF[TSL[TIME_SERIES_TYPE, SIZE]], key: int) -> REF[TIME_SERIES_TYPE]:
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
def tsl_get_item_ts(ts: REF[TSL[TIME_SERIES_TYPE, SIZE]], key: TS[int], _sz: Type[SIZE] = AUTO_RESOLVE) -> REF[TIME_SERIES_TYPE]:
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


@compute_node
def index_of(tsl: TSL[TIME_SERIES_TYPE, SIZE], ts: TIME_SERIES_TYPE) -> TS[int]:
    """
    Return the index of the leftmost time-series with the equal value to ts in the TSL
    """
    return next((i for i, t in enumerate(tsl) if t.valid and t.value == ts.value), -1)


@graph(overloads=add_)
def add_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise addition of TSL elements.  A missing value on either lhs or rhs causes a gap on the output
    """
    return TSL.from_ts(*(a + b for a, b in zip(lhs, rhs)))


@graph(overloads=sum_)
def sum_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Binary sum is addition.  TODO - should not be necessary when multi-arg sum is implemented
    """
    return lhs + rhs


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
def min_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise min() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    return TSL.from_ts(*(min_(a,  b) for a, b in zip(lhs, rhs)))


@graph(overloads=min_)
def min_tsl_unary(ts: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Minimum value in the TSB
    """
    return reduce(lambda a, b: min_(a,  b), ts, zero(tp, min_))


@graph(overloads=max_)
def max_tsls(lhs: TSL[TIME_SERIES_TYPE, SIZE], rhs: TSL[TIME_SERIES_TYPE, SIZE]) -> TSL[TIME_SERIES_TYPE, SIZE]:
    """
    Item-wise min() of the TSL elements. Missing elements on either side will cause a gap in the output
    """
    return TSL.from_ts(*(max_(a,  b) for a, b in zip(lhs, rhs)))


@graph(overloads=max_)
def max_tsl_unary(ts: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TIME_SERIES_TYPE:
    """
    Maximum value in the TSB
    """
    return reduce(lambda a, b: max_(a,  b), ts, zero(tp, max_))


@compute_node(overloads=str_)
def str_(ts: TSL[TIME_SERIES_TYPE, SIZE], tp: Type[TIME_SERIES_TYPE] = AUTO_RESOLVE) -> TS[str]:
    return str(ts.value)


@compute_node(valid=tuple(), overloads=union_tsl)
def union_tsl_tss(tsl: TSL[TSS[KEYABLE_SCALAR], SIZE], _output: TSS_OUT[KEYABLE_SCALAR] = None) -> TSS[KEYABLE_SCALAR]:
    tss: TSS[KEYABLE_SCALAR, SIZE]
    to_add: set[KEYABLE_SCALAR] = set()
    to_remove: set[KEYABLE_SCALAR] = set()
    for tss in tsl.modified_values():
        to_add |= tss.added()
        to_remove |= tss.removed()
    if (disputed := to_add.intersection(to_remove)):
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
