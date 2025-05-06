# Lifted operators from Python.

__all__ = ("round_",)

from hgraph._wiring._lift import lift
from hgraph._types._ts_type import TS
from hgraph._wiring._decorators import graph

_round = lift(round, inputs={"number": TS[float], "ndigits": TS[int]}, output=TS[float])


# Wraps the round operator
# This could be extended to an operator, or have additional behaviour such as significant place rounding
# as well as providing round rules (i.e. half-up/down, etc.)
@graph
def round_(ts: TS[float], n_digits: TS[int]) -> TS[float]:
    """
    Rounds the input to the have the n_digits decimal places.
    This is currently implemented using the python round function.
    """
    return _round(number=ts, ndigits=n_digits)
