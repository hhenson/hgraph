from hgraph._types._scalar_types import DEFAULT
from hgraph._wiring._decorators import operator
from hgraph._types import TIME_SERIES_TYPE, TS, OUT


__all__ = ["to_json", "from_json"]


@operator
def to_json(ts: DEFAULT[TIME_SERIES_TYPE]) -> TS[str]:
    """
    Converts the ``ts`` to a JSON string.
    """


@operator
def from_json(ts: TS[str]) -> OUT:
    """
    Converts the ``ts`` JSON string to the OUT type.
    Usage would be along the lines of:

    ::

        value = from_json[TS[MySchema]](ts)

    """
