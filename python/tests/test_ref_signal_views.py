"""REF-SAMPLE: designation signals and declared ordinary views stay distinct."""
import pytest

from hgraph import REF, SCALAR, SIGNAL, TIME_SERIES_TYPE, TS, compute_node, graph, if_, modified, sample, valid
from hgraph.test import eval_node


@compute_node(valid=(), active=("clock",))
def _metadata(value: SIGNAL, clock: TS[int]) -> TS[tuple[bool, bool]]:
    return value.valid, value.modified


_DESIGNATIONS = [REF[TS[int]], REF[TS[SCALAR]], SIGNAL]
_DECLARATIONS = [TS[int], TS[SCALAR], TIME_SERIES_TYPE, *_DESIGNATIONS]
_IDS = ["concrete-value", "scalar-generic", "series-generic",
        "reference", "generic-reference", "signal"]
_SELECT = [False, None, True, None, False, None, True, None]
_VALUE = [7, 8, None, 9, None, 10, None, None]


def _run(declaration, consume):
    def observe(value, clock):
        return consume(value, clock)

    observe.__annotations__ = {"value": declaration, "clock": TS[int]}
    observer = graph(observe)

    @graph
    def app(select: TS[bool], value: TS[int], clock: TS[int]):
        return observer(if_(select, value).true, clock)

    return eval_node(app, _SELECT, _VALUE, [1] * len(_SELECT))


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_declared_view_metadata_from_independent_clock(declaration):
    designation = declaration in _DESIGNATIONS
    expected = ([(True, True), (True, False)] * 4 if designation else
                [(False, False), (False, False), (True, True), (True, True),
                 (False, False), (False, False), (True, True), (True, False)])
    assert _run(declaration, _metadata) == expected


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_sample_uses_declared_view_including_selected_empty(declaration):
    designation = declaration in _DESIGNATIONS
    expected = ([1, None, 1, None, 1, None, 1, None] if designation else
                [None, None, 1, 1, None, None, 1, None])
    assert _run(declaration, lambda value, clock: sample(value, 1)) == expected


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_modified_uses_declared_view(declaration):
    designation = declaration in _DESIGNATIONS
    expected = ([True, False, True, False, True, False, True, False] if designation else
                [False, None, True, True, False, None, True, False])
    assert _run(declaration, lambda value, clock: modified(value)) == expected


def test_valid_of_explicit_reference_observes_empty_designations():
    # The reference-aware operator subscribes to designation changes. An
    # ordinary scalar alias has no wake on unbind; its validity is tested
    # above through an independent clock, rather than expecting a data tick.
    assert _run(REF[TS[int]], lambda value, clock: valid(value)) == [
        False, None, True, None, False, None, True, None]


@pytest.mark.parametrize("declaration", _DECLARATIONS[:3], ids=_IDS[:3])
def test_empty_retarget_clears_ordinary_metadata_after_old_target_ticks(declaration):
    def observe(value, clock):
        return _metadata(value, clock)

    observe.__annotations__ = {"value": declaration, "clock": TS[int]}
    observer = graph(observe)

    @graph
    def app(select: TS[bool], value: TS[int], clock: TS[int]):
        return observer(if_(select, value).true, clock)

    assert eval_node(app, [True, False], [7, 8], [1, 1]) == [
        (True, True), (False, False)]
