from functools import reduce

__all__ = (
    "scale_rank",
    "combine_ranks",
    "compare_ranks",
)

from typing import Iterable


def scale_rank(rank: dict[type, float], scale: float) -> dict[type, float]:
    """
    Rank scaling happens when generic-ness in nested, for example TS[X] will scale the rank of X to produce its own
    """
    return {k: v * scale for k, v in rank.items()}


def combine_ranks(ranks: Iterable[dict[type, float]], scale: float = None) -> dict[type, float]:
    """
    Combination of ranks is done by taking the minimum of the ranks for each type so the lower generic-ness is used
    For example a bundle of TS[X] and TS[Tuple[X, ...]] will be generic to X to the level of Tuple[X, ...]
    """
    reduction = reduce(min_rank, ranks, {})
    if scale is not None:
        return scale_rank(reduction, scale)
    else:
        return reduction


def min_rank(x, y):
    return {k: min(x.get(k, 1.0), y.get(k, 1.0)) for k in x.keys() | y.keys()}


def compare_ranks(rank1: dict[type, float], rank2: dict[type, float]) -> bool:
    """
    Comparison of ranks is done by comparing the sum of ranks by all type parameters and combining them
    """
    return sum(rank1.values(), 0.0) < sum(rank2.values(), 0.0)
