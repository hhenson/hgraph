from typing import Generic

import pytest
from frozendict import frozendict as fd

from hgraph import (
    component,
    TS,
    graph,
    TSD,
    map_,
    add_,
    GlobalState,
    set_record_replay_model,
    IN_MEMORY,
    RecordReplayContext,
    RecordReplayEnum,
    OUT,
    debug_print,
    MIN_ST,
    MIN_TD,
    TimeSeriesSchema,
    SCALAR,
    compute_node,
    RECORDABLE_STATE,
    TS_OUT,
    DEFAULT,
)
from hgraph.test import eval_node


def test_component():

    @component
    def my_component(ts: TS[float], key: str) -> TS[float]:
        return ts + 1.0

    assert eval_node(my_component, ts=[1.0, 2.0, 3.0], key="key_1") == [2.0, 3.0, 4.0]


def test_component_error_duplicate_id():
    @component
    def my_component(ts: TS[float], key: str) -> TS[float]:
        return ts + 1.0

    @graph
    def duplicate_wiring(ts: TS[float], key: str) -> TS[float]:
        a = my_component(ts=ts, key=key)
        b = my_component(ts=ts + 1, key=key)
        return a + b

    with pytest.raises(RuntimeError):
        assert eval_node(duplicate_wiring, ts=[1.0, 2.0, 3.0], key="key_1") == [2.0, 3.0, 4.0]


def test_recordable_id_from_ts():

    @component(recordable_id="Test_{key}")
    def my_component(ts: TS[float], key: TS[str]) -> TS[float]:
        return ts + 1.0

    assert eval_node(my_component, ts=[1.0, 2.0, 3.0], key=["key_1"]) == [2.0, 3.0, 4.0]


def test_record_replay():

    @component
    def my_component(a: TSD[str, TS[float]], b: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return map_(add_[TS[float]], a, b)

    with GlobalState() as gs:
        set_record_replay_model(IN_MEMORY)

        with RecordReplayContext(mode=RecordReplayEnum.RECORD):
            assert eval_node(my_component, a=[fd(a=1.0, b=2.0), fd(a=2.0)], b=[fd(a=3.0, b=2.0), fd(b=1.0)]) == [
                fd(a=4.0, b=4.0),
                fd(a=5.0, b=3.0),
            ]
        assert len(a_ts := gs.get(f":memory:my_component.a")) == 2
        assert a_ts == [
            (MIN_ST, fd(a=1.0, b=2.0)),
            (MIN_ST + MIN_TD, fd(a=2.0)),
        ]
        assert len(b_ts := gs.get(f":memory:my_component.b")) == 2
        assert b_ts == [
            (MIN_ST, fd(a=3.0, b=2.0)),
            (MIN_ST + MIN_TD, fd(b=1.0)),
        ]
        assert len(gs.get(f":memory:my_component.__out__")) == 2

        with RecordReplayContext(mode=RecordReplayEnum.REPLAY):
            assert eval_node(my_component, a=[], b=[]) == [
                fd(a=4.0, b=4.0),
                fd(a=5.0, b=3.0),
            ]

        with RecordReplayContext(mode=RecordReplayEnum.COMPARE):
            assert eval_node(my_component, a=[], b=[]) == [
                fd(a=4.0, b=4.0),
                fd(a=5.0, b=3.0),
            ]

        a_ts[0] = a_ts[0][0], fd(a=1.0, b=1.0)  # Reset one of the inputs

        with RecordReplayContext(mode=RecordReplayEnum.REPLAY):
            assert eval_node(my_component, a=[], b=[]) == [
                fd(a=4.0, b=3.0),
                fd(a=5.0, b=2.0),
            ]

        with pytest.raises(RuntimeError):
            with RecordReplayContext(mode=RecordReplayEnum.COMPARE):
                assert eval_node(my_component, a=[], b=[]) == [
                    fd(a=4.0, b=3.0),
                    fd(a=5.0, b=2.0),
                ]

        with RecordReplayContext(mode=RecordReplayEnum.RECOVER):
            assert eval_node(
                my_component,
                a=[None, None, None, None, fd(a=1.0, b=2.0)],
                b=[None, None, None, None, fd(a=6.0, b=7.0)],
                __start_time__=MIN_ST + MIN_TD * 3,
                __elide__=True,
            ) == [
                fd(a=5.0, b=2.0),
                fd(a=7.0, b=9.0),
            ]


def test_record_recovery():
    @component
    def my_component(a: TSD[str, TS[float]], b: TSD[str, TS[float]]) -> TSD[str, TS[float]]:
        return map_(add_[TS[float]], a, b)

    with GlobalState() as gs:
        set_record_replay_model(IN_MEMORY)

        with RecordReplayContext(mode=RecordReplayEnum.RECORD):
            assert eval_node(my_component, a=[fd(a=1.0, b=2.0), fd(a=2.0)], b=[fd(a=3.0, b=2.0), fd(b=1.0)]) == [
                fd(a=4.0, b=4.0),
                fd(a=5.0, b=3.0),
            ]

        with RecordReplayContext(mode=RecordReplayEnum.RECOVER | RecordReplayEnum.RECORD):
            assert eval_node(
                my_component,
                a=[None, None, None, None, fd(a=1.0, b=2.0)],
                b=[None, None, None, None, fd(a=6.0, b=7.0)],
                __start_time__=MIN_ST + MIN_TD * 3,
                __elide__=True,
            ) == [
                fd(a=5.0, b=3.0),
                fd(a=7.0, b=9.0),
            ]

            assert len(a_ts := gs.get(f":memory:my_component.a")) == 4
            assert len(b_ts := gs.get(f":memory:my_component.b")) == 4
            assert len(b_ts := gs.get(f":memory:my_component.__out__")) == 4


def test_recorded_state():

    class SimpleState(TimeSeriesSchema, Generic[SCALAR]):
        last_value_: TS[SCALAR]

    @compute_node()
    def de_dup_simple(ts: TS[SCALAR], _state: RECORDABLE_STATE[SimpleState[SCALAR]] = None) -> TS[SCALAR]:
        if not _state.last_value_.valid or _state.last_value_.value != ts.value:
            _state.last_value_.value = ts.value
            return ts.value

    @component(recordable_id="test_id")
    def simple_de_dup_component(ts: TS[int]) -> TS[int]:
        return de_dup_simple(ts, __recordable_id__="de_dup_1")

    assert eval_node(simple_de_dup_component, [1, 2, 3, 3, 4]) == [1, 2, 3, None, 4]
