"""Pinning tests for the upstream-compat export batch (2026-07-17):
utc_now, equal_lambdas/callable_shape_key, EvaluationClock (as annotation
injectable), get_recorded_value, get_context, TSW_OUT, is_feature_enabled."""
import os
from typing import TypeVar

import _hgraph
import hgraph as hg
from hgraph import TS, EvaluationClock, compute_node, graph
from hgraph.test import eval_node


def test_utc_now_is_naive():
    now = hg.utc_now()
    assert now.tzinfo is None


def test_optional_wiring_annotations_and_parameterized_objects_are_accepted():
    from typing import Generic

    value_type = TypeVar("value_type")

    class Box(Generic[value_type]):
        pass

    assert TS[Box[int]].handle.is_ts
    assert (TS[int] | None) == TS[int]
    assert hg.datetime is not None


def test_state_accepts_documentary_state_type():
    assert repr(hg.STATE[dict]).startswith("STATE[")


def test_time_series_bounded_python_type_var_is_a_generic_service_output():
    class Quote(hg.TimeSeriesSchema):
        value: TS[float]

    quote_type = TypeVar("quote_type", bound=hg.TSB[Quote])

    @hg.subscription_service
    def quote(key: TS[str], path: str = "quote") -> quote_type:
        ...

    assert quote.descriptor is None
    assert quote[quote_type: hg.TSB[Quote]].descriptor is not None


def test_wiring_graph_context_compatibility_boundary_is_available():
    @graph
    def identity(value: TS[int]) -> TS[int]:
        hg.WiringGraphContext.instance().build_services()
        return value

    assert eval_node(identity, [1]) == [1]


def test_equal_lambdas_shape_semantics():
    assert hg.equal_lambdas(lambda x: x + 1, lambda y: y + 1)      # names ignored
    assert not hg.equal_lambdas(lambda x: x + 1, lambda x: x + 2)  # constants kept
    def named(x):
        return x + 1
    assert not hg.equal_lambdas(lambda x: x + 1, named)  # non-lambda: identity


def test_evaluation_clock_annotation_injects():
    @compute_node
    def stamped(ts: TS[int], clock: EvaluationClock = None) -> TS[bool]:
        return clock.evaluation_time is not None

    assert eval_node(stamped, [1]) == [True]


def test_get_recorded_value_qualified():
    with hg.GlobalState():
        with hg.record_replay_scope(hg.RecordReplayEnum.RECORD):
            @graph
            def g(a: TS[int]):
                hg.record(a, key="out", recordable_id="nodes.record")

            eval_node(g, [1, 2])
        assert [v for _, v in hg.get_recorded_value()] == [1, 2]


def test_recorder_api_uses_active_global_state():
    recorder = object()
    with hg.GlobalContext(hg.GlobalState()) as state:
        hg.set_recorder_api(recorder)
        hg.set_recording_label("test")
        assert hg.get_recorder_api() is recorder
        assert hg.get_recording_label() == "test"
        assert state["__recorder_api__"] is recorder


def test_record_replay_reset_mode_is_exposed():
    assert hg.RecordReplayEnum.RESET == _hgraph.MODE_RESET
    assert hg.RecordReplayEnum.RESET & hg.RecordReplayEnum.RESET


def test_get_context_reads_published_context():
    @graph
    def inner() -> TS[int]:
        return hg.get_context[TS[int]]("answer") + 1

    @graph
    def outer(a: TS[int]) -> TS[int]:
        with hg.context("answer", a):
            return inner()

    assert eval_node(outer, [41]) == [42]


def test_get_context_missing():
    import pytest

    @graph
    def g(a: TS[int]) -> TS[int]:
        missing = hg.get_context[TS[int]]("nope")
        assert missing is None
        return a

    assert eval_node(g, [1]) == [1]

    @graph
    def g2(a: TS[int]) -> TS[int]:
        hg.get_context[TS[int]]("nope", required=True)
        return a

    with pytest.raises(hg.WiringError, match="required"):
        eval_node(g2, [1])


def test_tsw_out_maps_to_tsw():
    from hgraph import TSW, WindowSize

    assert hg.TSW_OUT[int, WindowSize[3]] == TSW[int, WindowSize[3]]


def test_collection_output_annotations_map_to_input_shapes():
    assert hg.TSD_OUT[str, TS[int]] == hg.TSD[str, TS[int]]
    assert hg.TSS_OUT[str] == hg.TSS[str]


def test_is_feature_enabled_env_var():
    assert hg.is_feature_enabled("compat_export_probe") is False
    os.environ["HGRAPH_COMPAT_EXPORT_PROBE"] = "true"
    try:
        # NB the upstream feature switch caches per-feature lookups; a fresh
        # name observes the env var.
        assert hg.is_feature_enabled("compat_export_probe2") is False
        os.environ["HGRAPH_COMPAT_EXPORT_PROBE3"] = "true"
        assert hg.is_feature_enabled("compat_export_probe3") is True
    finally:
        os.environ.pop("HGRAPH_COMPAT_EXPORT_PROBE", None)
        os.environ.pop("HGRAPH_COMPAT_EXPORT_PROBE3", None)


def test_tsb_absent_field_raises_attribute_error():
    """Harmonized with TSL: an absent bundle field is an ATTRIBUTE error."""
    import pytest
    from hgraph import TSB, TimeSeriesSchema

    class Pair(TimeSeriesSchema):
        x: TS[int]
        y: TS[int]

    @compute_node
    def probe(b: TSB[Pair]) -> TS[bool]:
        try:
            b.not_a_field
        except AttributeError:
            return True
        return False

    assert eval_node(probe, [{"x": 1, "y": 2}]) == [True]
