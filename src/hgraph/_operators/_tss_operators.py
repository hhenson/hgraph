

__all__ = ("compute_set_delta",)

from hgraph._types import SCALAR, TSS, SetDelta


def compute_set_delta(value: set[SCALAR], out: TSS[SCALAR] ) -> SetDelta[SCALAR]:
    """
    Compute the set delta between the value in the output and the set provided.
    This is useful when the compute node computes the target set, then it can use this function
    to compute the appropriate delta. This requires the output of the node and the new set.
    """
    from hgraph._impl._types._tss import PythonSetDelta
    value = value.value
    if out.valid:
        old_value = out.value
        added = value - old_value
        removed = old_value - value
        return PythonSetDelta(added, removed)
    else:
        return PythonSetDelta(value, set())
