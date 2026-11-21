import _hgraph

from hgraph import (
    REF,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    combine,
    compute_node,
    graph,
    last_modified_date,
    last_modified_time,
    last_modified_wall_clock_time,
    sample,
    try_except,
    switch_,
    wire,
)
from hgraph._wiring._core import _unwrap
from hgraph._wiring._graph import _as_wired
from hgraph.test import eval_node


class Reply(TimeSeriesSchema):
    status: TS[int]
    value: TS[float]


@graph
def make_reply(
    value: TS[float], repository: TSD[str, TSB[Reply]]
) -> TSB[Reply]:
    return combine[TSB[Reply]](status=1, value=value)


@graph
def forwarding_reply(
    value: TS[float], repository: TSD[str, TSB[Reply]]
) -> TSB[Reply]:
    return try_except(make_reply, value, repository).out


@graph
def referenced_reply(
    value: TS[float], repository: TSD[str, TSB[Reply]]
) -> TSB[Reply]:
    return repository["existing"]


@graph
def ref_switch(
    use_reference: TS[bool],
    value: TS[float],
    repository: TSD[str, TSB[Reply]],
) -> TSB[Reply]:
    cases = _hgraph.switch_cases(
        {False: _as_wired(forwarding_reply), True: _as_wired(referenced_reply)},
        key_type=_unwrap(use_reference).ts_type,
    )
    return wire(
        "switch_",
        use_reference,
        cases,
        value,
        repository,
        __output_type__=REF[TSB[Reply]],
    )


def test_ref_switch_preserves_forwarding_value_terminal():
    assert eval_node(
        ref_switch,
        [False],
        [2.5],
        [{"existing": {"status": 0, "value": 1.0}}],
    ) == [{"status": 1, "value": 2.5}]


@compute_node(active=("clock",), valid=("clock",))
def clocked_value(clock: TS[int], value: TS[int]) -> TS[tuple[bool, bool, int]]:
    return value.valid, value.modified, value.value if value.valid else 0


def test_switch_output_follows_a_silent_branch_withdrawal_and_recovery():
    @compute_node
    def echo(value: TS[int]) -> TS[int]:
        return value.value

    @compute_node
    def sometimes(value: TS[int], _output: TS[int] = None) -> TS[int]:
        if value.value < 0:
            _output.invalidate()
        elif value.value > 7:
            return value.value

    @graph
    def run(key: TS[int], value: TS[int], clock: TS[int]) -> TS[tuple[bool, bool, int]]:
        return clocked_value(clock, switch_(key, {1: echo, 2: sometimes}, value))

    keys = [1, None, 2, None, None, None, None, 1, 2, None, 1]
    values = [7, None, None, 9, -1, None, 13, 7, None, None, None]
    clock = list(range(len(keys)))
    assert eval_node(run, keys, values, clock) == [
        (True, True, 7), (True, False, 7), (False, False, 0), (True, True, 9),
        (False, False, 0), (False, False, 0), (True, True, 13), (True, True, 7),
        (False, False, 0), (False, False, 0), (True, True, 7),
    ]

    for timestamp in (last_modified_time, last_modified_date, last_modified_wall_clock_time):
        @graph
        def sampled(key: TS[int], value: TS[int], clock: TS[int]) -> TS[int]:
            selected = switch_(key, {1: echo, 2: sometimes}, value)
            return sample(timestamp(selected), clock)

        assert eval_node(sampled, keys, values, clock) == [
            0, None, None, 3, None, None, 6, 7, None, None, 10
        ]


def test_sampling_a_switch_that_returns_an_input_keeps_that_upstream_reference():
    @compute_node(active=("save",), valid=("save",))
    def sample_ref(save: TS[bool], value: REF[TS[int]]) -> REF[TS[int]]:
        return value.value

    @compute_node(active=("clock",), valid=("clock",))
    def read(clock: TS[int], live: TS[int], saved: TS[int]) -> TS[tuple[int, int]]:
        return live.value, saved.value

    @graph
    def run(key: TS[bool], a: TS[int], b: TS[int], save: TS[bool], clock: TS[int]) -> TS[tuple[int, int]]:
        selected = switch_(key, {False: lambda a, b: a, True: lambda a, b: b}, a, b)
        return read(clock, selected, sample_ref(save, selected))

    assert eval_node(run, [False, True, None, None, False], [7, None, 8], [4, None, None, 5],
                     [True], list(range(5))) == [(7, 7), (4, 7), (4, 8), (5, 8), (8, 8)]
