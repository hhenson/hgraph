import _hgraph

from hgraph import (
    REF,
    TS,
    TSB,
    TSD,
    TimeSeriesSchema,
    combine,
    graph,
    try_except,
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
