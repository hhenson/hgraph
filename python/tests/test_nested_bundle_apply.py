"""A Python node's result must apply with DELTA semantics at every nesting
level, including collections inside TSB children (python_bridge.rst,
"Per-tick application is registry-free"): the fused per-kind apply recurses
through each child's own erased apply, it does not fall back to whole-value
``from_python`` for bundle children. Golden values verified against the
0.8.14 wheel's delta pipeline."""

from hgraph import (
    REMOVE,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    compute_node,
    eval_node,
    graph,
)


class _InnerBundle(TimeSeriesSchema):
    field: TSD[str, TS[int]]


@compute_node
def _emit_nested(ts: TS[int]) -> TSD[str, TSB[_InnerBundle]]:
    i = ts.value
    if i == 0:
        return {"outer": {"field": {"x": 1, "y": 2}}}
    if i == 1:
        return {"outer": {"field": {"y": 20}}}
    if i == 2:
        return {"outer": {"field": {"x": REMOVE}}}
    return {"outer": {"field": {"z": 30}}}


@graph
def _nested_graph(ts: TS[int]) -> TSD[str, TSB[_InnerBundle]]:
    return _emit_nested(ts)


def test_tsd_of_bundle_applies_nested_collections_as_deltas():
    assert eval_node(_nested_graph, [0, 1, 2, 3]) == [
        {"outer": {"field": {"x": 1, "y": 2}}},
        # A dict for the inner TSD is a DELTA: updating y must not remove x.
        {"outer": {"field": {"y": 20}}},
        # An authored REMOVE reaches the inner TSD as a removal.
        {"outer": {"field": {"x": REMOVE}}},
        {"outer": {"field": {"z": 30}}},
    ]
