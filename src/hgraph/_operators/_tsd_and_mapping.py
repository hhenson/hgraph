from typing import Tuple, Mapping, TypeVar

from hgraph._types import TS, DEFAULT, TSD, K, K_1
from hgraph._types._time_series_types import TIME_SERIES_TYPE, TIME_SERIES_TYPE_1, OUT
from hgraph._wiring._decorators import operator

__all__ = (
    "collapse_keys",
    "flip",
    "flip_keys",
    "keys_",
    "partition",
    "rekey",
    "uncollapse_keys",
    "unpartition",
    "values_",
)

TSD_OR_MAPPING = TypeVar("TSD_OR_MAPPING", TSD, TS[Mapping])


@operator
def keys_(ts: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    Returns the TSS or set of keys in a dictionary.
    """


@operator
def values_(ts: TIME_SERIES_TYPE) -> DEFAULT[OUT]:
    """
    Returns a tuple of the values in the dictionary.
    One options for a TSD, the values_ can be applied when the time-series value is a suitable TS[SCALAR],
    where the results are returned as a set.
    In this case use values_[TSS[...]](tsd)
    """


@operator
def rekey(ts: TIME_SERIES_TYPE, new_keys: TIME_SERIES_TYPE_1) -> DEFAULT[OUT]:
    """
    Re-keys the input time series to the provided key.
    """


@operator
def flip(ts: TIME_SERIES_TYPE, **kwargs) -> DEFAULT[OUT]:
    """
    Flips the dictionary so that the values become the keys and the keys become the values.
    Params:
    * unique: bool - if False, collects multiple keys into a TSS output instead of a single TS
    """


@operator
def partition(ts: TIME_SERIES_TYPE, partitions: TIME_SERIES_TYPE_1) -> OUT:
    """
    Splits a TSD into multiple TSDs given a mapping TSD. Its output is a TSD[K1, TSD[K, V]] for inputs of TSD[K, V]
    and TSD[K, K1] for mapping
    """


@operator
def unpartition(ts: TIME_SERIES_TYPE) -> OUT:
    """
    Takes a nested TSD[K1, TSD[K, V]] and produces a TSD[K, V] by merging the inner TSDs
    """


@operator
def flip_keys(ts: TIME_SERIES_TYPE) -> OUT:
    """
    Work on nested TSDs like TSD[K, TSD[K1, V]] to inverse keys to get TSD[K1, TSD[K, V]]
    """


@operator
def collapse_keys(ts: TIME_SERIES_TYPE) -> OUT:
    """
    Given a nested TSD[K, TSD[K1, V]] collapse_keys will produce TSD[Tuple[K, K1], V] where the keys are pairs of
    outer and inner key for each value
    """


@operator
def uncollapse_keys(
    ts: TSD[Tuple[K, K_1], TIME_SERIES_TYPE], remove_empty: bool = True
) -> TSD[K, TSD[K_1, TIME_SERIES_TYPE]]:
    """
    Given a TSD[Tuple[K, K1], V] uncollapse_keys will produce a nested TSD[K, TSD[K1, V]]. It is the reverse operation
    to ``collapse_keys``
    """
