"""Standard arrow helpers — port of upstream ``hgraph.arrow._std_operators``."""
import hgraph as hg
from hgraph import TS

from .._wiring import compute_node
from ._arrow import arrow

__all__ = ("const_", "apply_", "c")


def const_(first, second=None, tp=None):
    """Inject constant value(s) into the arrow flow (``i / const_(1)``)."""

    def _build(_, first_=first, second_=second, tp_=tp):
        tp_ = tp_ if tp_ is None or type(tp_) is tuple else (tp_,)
        from ._arrow import _build_inputs

        return _build_inputs(first_, second_, type_map=tp_)

    return arrow(_build, __name__=f"{first}" if second is None else f"{first}, {second}")


c = const_  # Use c when it will not be confusing.

def apply_(tp):
    """Applies the function in the first element of a pair to the value in
    the second; ``tp`` is the output type."""
    output_type = tp if hasattr(tp, "handle") else TS[tp]
    return arrow(lambda pair: hg.apply[output_type](pair[0], pair[1]),
                 __name__=f"apply_({tp})")
