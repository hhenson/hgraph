from statistics import variance, stdev
from typing import Tuple, Set
from typing import Type

from frozendict import frozendict

from hgraph._impl._types._tss import PythonSetDelta
from hgraph._operators import sub_, getitem_, min_, max_, sum_, mean, var, str_, values_, rekey, flip, partition, \
    flip_keys, collapse_keys, uncollapse_keys, zero, std, keys_, union
from hgraph._types._scalar_types import KEYABLE_SCALAR, SCALAR
from hgraph._types._time_series_types import OUT, K, K_1
from hgraph._types._ts_type import TS, TS_OUT
from hgraph._types._tsl_type import TSL, SIZE
from hgraph._types._tss_type import TSS
from hgraph._types._type_meta_data import AUTO_RESOLVE
from hgraph._wiring._decorators import compute_node, graph
from hgraph._wiring._wiring_errors import WiringError


__all__ = ()


@compute_node(overloads=union)
def union_frozendicts(
        lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]], rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]
) -> TS[frozendict[KEYABLE_SCALAR, SCALAR]]:
    """
    Combine two timeseries of frozendicts
    """
    return lhs.value | rhs.value


@compute_node(overloads=sub_)
def sub_frozendicts(
        lhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]], rhs: TS[frozendict[KEYABLE_SCALAR, SCALAR]]
) -> TS[frozendict[KEYABLE_SCALAR, SCALAR]]:
    """
    Return the difference of the two frozendicts (by key)
    """
    return frozendict({k: v for k, v in lhs.value.items() if k not in rhs.value})


@compute_node(overloads=getitem_)
def getitem_frozendict(
        ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], key: TS[KEYABLE_SCALAR], default_value: TS[SCALAR] = None
) -> TS[SCALAR]:
    """
    Retrieve the dict item by key from the timeseries of scalar frozen dicts
    """
    default = default_value.value if default_value.valid else None
    return ts.value.get(key.value, default)


@graph(overloads=min_)
def min_frozendict(
        *ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE], default_value: TS[SCALAR] = None
) -> TS[SCALAR]:
    if len(ts) == 1:
        return min_frozendict_unary(ts[0], default_value)
    else:
        raise WiringError(f"Cannot compute min of {len(ts)} frozendicts")


@compute_node
def min_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Return the minimum value in the frozendict
    If the frozendict is empty, the default value is returned
    """
    return min(ts.value.values(), default=default_value.value)


@graph(overloads=max_)
def max_frozendict(
        *ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE], default_value: TS[SCALAR] = None
) -> TS[SCALAR]:
    if len(ts) == 1:
        return max_frozendict_unary(ts[0], default_value)
    else:
        raise WiringError(f"Cannot compute max of {len(ts)} frozen dicts")


@compute_node
def max_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], default_value: TS[SCALAR] = None) -> TS[SCALAR]:
    """
    Return the maximum value in the frozendict
    If the frozendict is empty, the default value is returned
    """
    return max(ts.value.values(), default=default_value.value)


@graph(overloads=sum_)
def sum_frozendict(
        *ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE], tp: Type[TS[SCALAR]] = AUTO_RESOLVE
) -> TS[SCALAR]:
    if len(ts) == 1:
        return _sum_frozendict_unary(ts[0], zero(tp, sum_))
    else:
        raise WiringError(f"Cannot compute sum of {len(ts)} frozen dicts")


@compute_node
def _sum_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]], zero_ts: TS[SCALAR]) -> TS[SCALAR]:
    """
    Return the sum of values in the frozendict value of the timeseries
    If the frozendict is empty, the default value is returned
    """
    return sum(ts.value.values(), start=zero_ts.value)


@graph(overloads=mean)
def mean_frozendict(*ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return _mean_frozendict_unary(ts[0])
    else:
        raise WiringError(f"Cannot compute mean of {len(ts)} frozen dicts")


@compute_node
def _mean_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[float]:
    """
    Return the mean of values in the frozendict value of the timeseries
    """
    ts = ts.value
    len_ts = len(ts)
    if len_ts == 0:
        return float("NaN")
    elif len_ts == 1:
        return next(iter(ts.values()))
    else:
        return sum(ts.values()) / len_ts


@graph(overloads=std)
def std_frozendict(*ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return _std_frozendict_unary(ts[0])
    else:
        raise WiringError(f"Cannot compute standard deviation of {len(ts)} frozen dicts")


@compute_node
def _std_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[float]:
    """
    Return the mean of values in the frozendict value of the timeseries
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(stdev(ts.values()))


@graph(overloads=var)
def var_frozendict(*ts: TSL[TS[frozendict[KEYABLE_SCALAR, SCALAR]], SIZE]) -> TS[float]:
    if len(ts) == 1:
        return _var_frozendict_unary(ts[0])
    else:
        raise WiringError(f"Cannot compute standard deviation of {len(ts)} frozen dicts")


@compute_node
def _var_frozendict_unary(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[float]:
    """
    Return the variance of values in the frozendict value of the timeseries
    """
    ts = ts.value
    if len(ts) <= 1:
        return 0.0
    else:
        return float(variance(ts.values()))


@compute_node(overloads=str_)
def str_frozendict(ts: TS[frozendict[KEYABLE_SCALAR, SCALAR]]) -> TS[str]:
    return str(dict(ts.value))


@compute_node(
    overloads=keys_,
    requires=lambda m, s: m[OUT].py_type in (TS[Set], TS[set], TS[frozenset])
                          or m[OUT].matches_type(TS[Set[m[K].py_type]]),
    resolvers={OUT: lambda m, s: TS[Set[m[K].py_type]]},
)
def keys_frozendict_as_set(ts: TS[frozendict[K, SCALAR]]) -> TS[Set[K]]:
    return set(ts.value.keys())


@compute_node(
    overloads=keys_,
    requires=lambda m, s: m[OUT].py_type is TSS or m[OUT].matches_type(TSS[m[K].py_type]),
    valid=("ts",),
)
def keys_frozendict_as_tss(ts: TS[frozendict[K, SCALAR]], _output: TS_OUT[Set[K]] = None) -> TSS[K]:
    prev = _output.value if _output.valid else set()
    new = set(ts.value.keys()) if ts.modified else set()
    return PythonSetDelta(new - prev, prev - new)


@compute_node(overloads=values_)
def values_frozendict_as_tuple(ts: TS[frozendict[K, SCALAR]]) -> TS[Tuple[SCALAR, ...]]:
    return tuple(ts.value.values())


@compute_node(overloads=rekey)
def rekey_frozendict(ts: TS[frozendict[K, SCALAR]], new_keys: TS[frozendict[K, K_1]]) -> TS[frozendict[K_1, SCALAR]]:
    new_keys_value = new_keys.value
    return {new_keys_value[k]: v for k, v in ts.value.items() if k in new_keys_value}


@compute_node(overloads=flip)
def flip_frozendict(ts: TS[frozendict[K, K_1]]) -> TS[frozendict[K_1, K]]:
    """As values may be repeated in the original dict it is indeterminate which key will be used as the value in the
    flipped dict"""
    return {v: k for k, v in ts.value.items()}


@compute_node(overloads=partition)
def partition_frozendict(
        ts: TS[frozendict[K, SCALAR]], partitions: TS[frozendict[K, K_1]]
) -> TS[frozendict[K_1, frozendict[K, SCALAR]]]:
    partitions_value = partitions.value
    out = {}
    for k, v in ts.value.items():
        partition_key = partitions_value.get(k)
        if partition_key:
            out.setdefault(partition_key, {})[k] = v
    return out


@compute_node(overloads=flip_keys)
def flip_keys_frozendict(ts: TS[frozendict[K, frozendict[K_1, SCALAR]]]) -> TS[frozendict[K_1, frozendict[K, SCALAR]]]:
    out = {}
    for k, v in ts.value.items():
        for k1, v1 in v.items():
            out.setdefault(k1, {})[k] = v1
    return out


@compute_node(overloads=collapse_keys)
def collapse_keys_frozendict(ts: TS[frozendict[K, frozendict[K_1, SCALAR]]]) -> TS[frozendict[Tuple[K, K_1], SCALAR]]:
    return {(k, k1): v for k, v1 in ts.value.items() for k1, v in v1.items()}


@compute_node(overloads=uncollapse_keys)
def uncollapse_keys_frozendict(ts: TS[frozendict[Tuple[K, K_1], SCALAR]]) -> TS[frozendict[K, frozendict[K_1, SCALAR]]]:
    out = {}
    for (k, k1), v in ts.value.items():
        out.setdefault(k, {})[k1] = v
    return out
