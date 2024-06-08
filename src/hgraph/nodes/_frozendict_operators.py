from typing import Type

from frozendict import frozendict

from hgraph import compute_node, add_, TS, KEYABLE_SCALAR, SCALAR, sub_, getitem_, min_, max_, sum_, str_, graph, zero, \
    AUTO_RESOLVE


@compute_node
def union_frozendicts(lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]],
                    rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[frozendict[KEYABLE_SCALAR, SCALAR]]:
    """
    Combine two timeseries of frozendicts
    """
    return {**lhs.value, **rhs.value}


@compute_node(overloads=sub_)
def sub_frozendicts(lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]],
                    rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[frozendict[KEYABLE_SCALAR, SCALAR]]:
    """
    Return the difference of the two frozendicts (by key)
    """
    return frozendict({k: v for k, v in lhs.value.items() if k not in rhs.value})


@compute_node(overloads=getitem_)
def getitem_frozendict(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]],
                       key: TS[KEYABLE_SCALAR],
                       default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Retrieve the dict item by key from the timeseries of scalar frozen dicts
    """
    default = default_value.value if default_value.valid else None
    return ts.value.get(key.value, default)


@compute_node(overloads=min_)
def min_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Return the minimum value in the frozendict
    If the frozendict is empty, the default value is returned
    """
    return min(ts.value.values(), default=default_value.value)


@compute_node(overloads=max_)
def max_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Return the maximum value in the frozendict
    If the frozendict is empty, the default value is returned
    """
    return max(ts.value.values(), default=default_value.value)


@graph(overloads=sum_)
def sum_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]],
                         tp: Type[TS[SCALAR]] = AUTO_RESOLVE) -> TS[SCALAR]:
    return _sum_frozendict_unary(ts, zero(tp, sum_))


@compute_node
def _sum_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], zero_ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Return the sum of values in the frozendict value of the timeseries
    If the frozendict is empty, the default value is returned
    """
    return sum(ts.value.values(), start=zero_ts.value)


@compute_node(overloads=str_)
def str_frozendict(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[str]:
    return str(dict(ts.value))
