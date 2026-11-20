"""TS-18 and REF-SAMPLE: every consumer that does not declare REF dereferences.

A SIGNAL, a concrete or generic ordinary parameter, and a REF-declared graph
parameter handed on to a non-REF consumer all observe the designated output:
its validity, ticks and samples. Only a consumer that itself declares REF
observes designations.
"""
import pytest

from hgraph import (REF, SCALAR, SIGNAL, TIME_SERIES_TYPE, TS, TSB, TSD, TSL, TSS, TSW, TS_OUT,
                    Size, WindowSize, TimeSeriesSchema, combine, compute_node, graph,
                    if_, last_modified_date, last_modified_time, last_modified_wall_clock_time,
                    modified, sample, valid)
from hgraph.test import eval_node


@compute_node(valid=(), active=("clock",))
def _metadata(value: SIGNAL, clock: TS[int]) -> TS[tuple[bool, bool]]:
    return value.valid, value.modified


@compute_node(valid=(), active=("clock",))
def _designation(value: REF[TS[int]], clock: TS[int]) -> TS[tuple[bool, bool]]:
    return value.valid, value.modified


_DECLARATIONS = [TS[int], TS[SCALAR], TIME_SERIES_TYPE, REF[TS[int]], REF[TS[SCALAR]], SIGNAL]
_IDS = ["concrete-value", "scalar-generic", "series-generic",
        "reference", "generic-reference", "signal"]
_SELECT = [False, None, True, None, False, None, True, None]
_VALUE = [7, 8, None, 9, None, 10, None, None]
# What every non-REF consumer sees: the designated output's validity, its
# sampled bind and its ticks, and nothing while the designation is empty.
_FOLLOWED_METADATA = [(False, False), (False, False), (True, True), (True, True),
                      (False, False), (False, False), (True, True), (True, False)]
_FOLLOWED_SAMPLE = [None, None, 1, 1, None, None, 1, None]
_FOLLOWED_MODIFIED = [False, None, True, True, False, None, True, False]
# What only a REF-declared consumer sees: designation publications, the
# first of them empty, and no wake for target ticks.
_DESIGNATION_METADATA = [(True, True), (True, False)] * 4


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
def test_signal_metadata_follows_the_designated_output(declaration):
    assert _run(declaration, _metadata) == _FOLLOWED_METADATA


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_sample_follows_the_designated_output(declaration):
    assert _run(declaration, lambda value, clock: sample(value, 1)) == _FOLLOWED_SAMPLE


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_modified_follows_the_designated_output(declaration):
    assert _run(declaration, lambda value, clock: modified(value)) == _FOLLOWED_MODIFIED


def test_only_a_ref_consumer_observes_designations():
    assert _run(REF[TS[int]], _designation) == _DESIGNATION_METADATA


def test_valid_of_explicit_reference_observes_empty_designations():
    # The reference-aware operator follows designation changes, including empty ones.
    assert _run(REF[TS[int]], lambda value, clock: valid(value)) == [
        False, None, True, None, False, None, True, None]


@pytest.mark.parametrize("declaration", _DECLARATIONS, ids=_IDS)
def test_empty_retarget_clears_metadata_after_old_target_ticks(declaration):
    def observe(value, clock):
        return _metadata(value, clock)

    observe.__annotations__ = {"value": declaration, "clock": TS[int]}
    observer = graph(observe)

    @graph
    def app(select: TS[bool], value: TS[int], clock: TS[int]):
        return observer(if_(select, value).true, clock)

    assert eval_node(app, [True, False], [7, 8], [1, 1]) == [
        (True, True), (False, False)]


class _ReferencePair(TimeSeriesSchema):
    left: REF[TS[int]]
    right: REF[TS[int]]


class _ValuePair(TimeSeriesSchema):
    left: TS[int]
    right: TS[int]


@pytest.mark.parametrize("shape", ["bundle", "fixed-list"])
def test_assembled_signal_follows_declared_reference_children(shape):
    def ordinary(value, clock):
        return _metadata(value, clock)

    ordinary.__annotations__ = {
        "value": TSB[_ValuePair] if shape == "bundle" else TSL[TS[int], Size[2]],
        "clock": TS[int],
    }
    observe_ordinary = graph(ordinary)

    @graph
    def app(select: TS[bool], value: TS[int], clock: TS[int]):
        selected = if_(select, value).true
        assembled = (TSB[_ReferencePair].from_ts(left=selected, right=selected)
                     if shape == "bundle" else
                     TSL[REF[TS[int]], Size[2]].from_ts(selected, selected))
        return combine(direct=_metadata(assembled, clock),
                       declared=observe_ordinary(assembled, clock))

    # A SIGNAL over an assembled structure follows its REF children exactly
    # as the ordinary declared view does; a repeated equal designation is
    # not a tick (TS-16), a target tick is (TS-18).
    followed = [(False, False), (False, False), (True, True), (True, True),
                (False, False), (False, False), (True, True), (True, False)]
    repeated_designations = [False, False, True, True, False, False, True, True]
    assert eval_node(app, repeated_designations, _VALUE, [1] * len(_SELECT)) == [
        {"direct": metadata, "declared": metadata} for metadata in followed]


class _TimestampBundle(TimeSeriesSchema):
    value: TS[int]
    sibling: TS[int]


_TIMESTAMPS = [last_modified_time, last_modified_wall_clock_time, last_modified_date]


@pytest.mark.parametrize("schema, payload", [
    (TS[int], lambda n: n),
    (TSS[int], lambda n: frozenset() if n == 0 else {n}),
    (TSD[int, TS[int]], lambda n: {} if n == 0 else {0: n}),
    (TSL[TS[int], Size[1]], lambda n: {0: n}),
    (TSL[TS[int], Size[-1]], lambda n: {0: n}),
    (TSB[_TimestampBundle], lambda n: {"value": n}),
    (TSW[int, WindowSize[2], WindowSize[1]], lambda n: n),
], ids=["scalar", "set", "dictionary", "fixed-list", "growing-list", "bundle", "window"])
@pytest.mark.parametrize("timestamp", _TIMESTAMPS)
def test_owner_invalidation_withdraws_the_timestamp_without_sampling(timestamp, schema, payload):
    @compute_node
    def source(command: TS[int], _output: TS_OUT[schema] = None) -> schema:
        if command.value < 0:
            _output.invalidate()
        else:
            return payload(command.value)

    @graph
    def sample_on_timestamp(command: TS[int], price: TS[int]) -> TS[int]:
        return sample(timestamp(source(command)), price)

    @graph
    def observe_timestamp(command: TS[int], clock: TS[int]) -> TS[tuple[bool, bool]]:
        return _metadata(timestamp(source(command)), clock)

    # Empty set/dictionary publications tick, as do equal scalar writes.
    first = 0 if schema in (TSS[int], TSD[int, TS[int]]) else 7
    last = first if schema == TS[int] else 8
    commands = [None, first, None, -1, -1, None, first, last]
    prices = [10, None, 20, None, None, 30, None, 40]
    assert eval_node(sample_on_timestamp, commands, prices) == [None, 10, None, None, None, None, 30, 40]
    assert eval_node(observe_timestamp, commands, [1] * len(commands)) == [
        (False, False), (True, True), (True, False), (False, False),
        (False, False), (False, False), (True, True), (True, True),
    ]


@pytest.mark.parametrize("timestamp", _TIMESTAMPS)
def test_timestamp_preserves_sampled_reference_transitions(timestamp):
    assert _run(SIGNAL, lambda value, clock: sample(timestamp(value), clock)) == [
        None, None, 1, 1, 1, None, 1, None,
    ]


@pytest.mark.parametrize("timestamp", _TIMESTAMPS)
def test_timestamp_observes_a_bundle_update_when_only_one_field_clears(timestamp):
    @compute_node
    def source(command: TS[int], _output: TS_OUT[TSB[_TimestampBundle]] = None) -> TSB[_TimestampBundle]:
        if command.value < 0:
            _output["value"].invalidate()
        else:
            return {"value": command.value, "sibling": 11}

    @graph
    def app(command: TS[int], price: TS[int]) -> TS[int]:
        return sample(timestamp(source(command)), price)

    assert eval_node(app, [7, -1, None, 8], [5, 6, 7, 8]) == [5, 6, None, 8]
