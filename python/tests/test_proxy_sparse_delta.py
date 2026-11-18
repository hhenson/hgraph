"""Sparse reference-dictionary deltas retain earlier recorded values."""

import hgraph as hg
from hgraph.test import eval_node


@hg.graph
def _identity(value: hg.TS[int]) -> hg.TS[int]:
    return value


@hg.compute_node
def _observe(value: hg.TSD[int, hg.TS[int]]) -> hg.TS[str]:
    modified = tuple(sorted((key, child.value) for key, child in value.modified_items()))
    removed = tuple(sorted(value.removed_keys()))
    delta = tuple(sorted(value.delta_value.items()))
    return repr((modified, removed, delta))


@hg.graph
def _mapped(value: hg.TSD[int, hg.TS[int]]) -> hg.TSD[int, hg.TS[int]]:
    return hg.map_(_identity, value)


@hg.graph
def _observed(value: hg.TSD[int, hg.TS[int]]) -> hg.TS[str]:
    return _observe(_mapped(value))


def test_proxy_delta_export_and_input_iteration_preserve_sparse_changes():
    initial = {key: key for key in range(128)}
    changes = [initial, {64: 900}, None, {64: hg.REMOVE}, {64: 901}]
    assert eval_node(_mapped, changes) == changes
    expected = [
        repr((tuple(sorted(initial.items())), (), tuple(sorted(initial.items())))),
        repr((((64, 900),), (), ((64, 900),))),
        None,
        repr(((), (64,), ((64, hg.REMOVE),))),
        repr((((64, 901),), (), ((64, 901),))),
    ]
    assert eval_node(_observed, changes) == expected
