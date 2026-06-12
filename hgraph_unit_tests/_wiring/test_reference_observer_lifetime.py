from hgraph import (
    REMOVE_IF_EXISTS,
    TimeSeriesSchema,
    TSB,
    TS,
    TSD,
    combine,
    const,
    graph,
    map_,
    nothing,
    switch_,
    take,
)
from hgraph.test import eval_node


def test_cascaded_switch_in_map_ticking_into_take():
    class B(TimeSeriesSchema):
        direct: TSD[str, TS[float]]
        gated: TSD[str, TS[float]]

    class C(TimeSeriesSchema):
        item: TSB[B]
        items: TSD[str, TSB[B]]

    @graph
    def build_b(values: TSD[str, TS[float]], inner_gate: TS[bool]) -> TSB[B]:
        plus_one = map_(lambda value: value + 1.0, values)
        gated_values = switch_(
            inner_gate,
            {
                True: lambda v: v,
                False: lambda v: nothing(TSD[str, TS[float]]),
            },
            plus_one,
        )
        return combine[TSB[B]](
            direct=plus_one,
            gated=gated_values,
        )

    @graph
    def build_c(branches: TSD[str, TSD[str, TS[float]]], inner_gate: TS[bool]) -> TSB[C]:
        bs = map_(build_b, branches, inner_gate)
        return combine[TSB[C]](
            item=bs[const("branch_0")],
            items=bs,
        )

    @graph
    def switched(branches: TSD[str, TSD[str, TS[float]]], outer_gate: TS[bool], inner_gate: TS[bool]) -> TSB[C]:
        return switch_(
            outer_gate,
            {
                True: lambda current_branches, current_inner: build_c(current_branches, current_inner),
                False: lambda current_branches, current_inner: build_c(current_branches, current_inner),
            },
            branches,
            inner_gate,
        )

    @graph
    def g(
        branches: TSD[str, TSD[str, TS[float]]],
        outer_gate: TS[bool],
        inner_gate: TS[bool],
    ) -> TSD[str, TSB[B]]:
        frame = switched(branches, outer_gate, inner_gate)
        return take(frame.items, 1)

    keys = [f"k{i:02d}" for i in range(5)]
    base_values = {key: float(i + 1) for i, key in enumerate(keys)}

    result = eval_node(
        g,
        [
            {
                "branch_0": base_values,
                "branch_1": base_values,
                "branch_2": base_values,
            },
            None,
            None,
            {
                "branch_0": {key: REMOVE_IF_EXISTS for key in keys[:2]},
                "branch_1": {key: REMOVE_IF_EXISTS for key in keys[:2]},
            },
            None,
            {
                "branch_0": {"extra": 10.0},
                "branch_1": {"extra": 10.0},
                "branch_2": {
                    keys[0]: REMOVE_IF_EXISTS,
                    "extra": 10.0,
                },
            },
        ],
        [
            False,
            None,
            True,
            None,
            None,
            None,
        ],
        [
            False,
            None,
            None,
            None,
            True,
            None,
        ]
    )

    result_updates = [update for update in result if update]
    expected_direct = {key: value + 1.0 for key, value in base_values.items()}

    assert len(result_updates) == 1
    assert set(result_updates[0].keys()) == {"branch_0", "branch_1", "branch_2"}
    for branch in result_updates[0].values():
        assert branch["direct"] == expected_direct
