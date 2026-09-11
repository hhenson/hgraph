from hgraph import (
    TIME_SERIES_TYPE,
    TS,
    TSD,
    TSS,
    graph,
    map_,
    mesh_,
    pass_through,
)
from hgraph import WiringError
from hgraph.test import eval_node

import pytest


@graph
def _generic_peer(
    key: TS[str],
    link: TS[str],
    values: TSD[str, TIME_SERIES_TYPE],
) -> TIME_SERIES_TYPE:
    return mesh_("generic-peer")[link]


@graph
def _generic_mesh(
    links: TSD[str, TS[str]],
    values: TSD[str, TS[int]],
) -> TSD[str, TS[int]]:
    return mesh_(
        _generic_peer,
        links,
        pass_through(values),
        __name__="generic-peer",
    )


def test_mesh_resolves_a_generic_declared_output_before_wiring_peer_lookups():
    assert eval_node(_generic_mesh, [{}], [{}]) is None


@graph
def _generic_nested_peer(
    key: TS[str],
    links: TSS[str],
    value: TSD[str, TIME_SERIES_TYPE],
    values: TSD[str, TSD[str, TIME_SERIES_TYPE]],
) -> TSD[str, TIME_SERIES_TYPE]:
    return map_(
        lambda key: mesh_("generic-nested-peer")[key],
        __keys__=links,
    )


@graph
def _generic_nested_mesh(
    links: TSD[str, TSS[str]],
    values: TSD[str, TSD[str, TS[int]]],
) -> TSD[str, TSD[str, TS[int]]]:
    return mesh_(
        _generic_nested_peer,
        links,
        values,
        pass_through(values),
        __name__="generic-nested-peer",
    )


def test_generic_mesh_scope_is_available_to_nested_map_output_inference():
    """The nested mesh scope resolves, and the graph is then correctly rejected.

    ``_generic_nested_peer`` returns one nesting level deeper than
    ``_generic_nested_mesh`` declares: mapping over the mesh elements yields
    ``TSD[str, TSD[str, TS[int]]]`` per peer, so the mesh is
    ``TSD[str, TSD[str, TSD[str, TS[int]]]]`` against a declared
    ``TSD[str, TSD[str, TS[int]]]``.

    Released hgraph rejects the same graph -- at the inner peer, reporting
    ``'TSD[str, TS[int]]' but 'TSD[str, REF[TSD[str, TS[int]]]]'`` -- so
    rejecting is the parity-matching outcome. It used to wire here only because
    the declared output was never enforced (issue #811). Reaching the type
    error at all still proves the mesh scope was available to the nested map's
    output inference, which is what this case exists to cover; the sibling
    above pins the non-nested resolution.
    """
    with pytest.raises(WiringError, match="declares its output as"):
        eval_node(_generic_nested_mesh, [{}], [{}])
