from hgraph import compute_node, TS


__all__ = ("not_",)

@compute_node
def not_(ts: TS[bool]) -> TS[bool]:
    return not ts.value