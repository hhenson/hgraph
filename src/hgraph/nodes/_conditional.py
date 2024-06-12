from hgraph import TS, compute_node, TIME_SERIES_TYPE

__all__ = ("filter_",)


@compute_node
def filter_(condition: TS[bool], ts: TIME_SERIES_TYPE) -> TIME_SERIES_TYPE:
    if condition.value:
        return ts.value if condition.modified else ts.delta_value