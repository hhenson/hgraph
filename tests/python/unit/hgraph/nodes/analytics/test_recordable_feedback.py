from datetime import datetime, timedelta

from hgraph import graph, TS, evaluate_graph, GraphConfiguration, MIN_TD, GlobalState, \
    compound_scalar, lag, const
from hgraph._operators._flow_control import merge
from hgraph.nodes.analytics import recordable_feedback, register_recorder_api, PolarsRecorderAPI
from hgraph.nodes.analytics._recorder_api import set_recording_label


@graph
def g() -> TS[int]:
    fb = recordable_feedback("test", TS[int])
    v = merge(fb(), const(0))
    v += 1
    fb(lag(v, period=timedelta(days=1)))
    return v


def test_recordable_feedback():
    with GlobalState():
        set_recording_label("test")
        register_recorder_api(api := PolarsRecorderAPI())
        api.create_or_update_table_definition("test", compound_scalar(value=int))
        dt = datetime(2024, 1, 1)
        result = evaluate_graph(g, GraphConfiguration(start_time=dt, end_time=dt + timedelta(days=4)))
        assert result == [(dt + timedelta(days=0) + MIN_TD * 0, 1),
                          (dt + timedelta(days=1) + MIN_TD * 1, 2),
                          (dt + timedelta(days=2) + MIN_TD * 2, 3),
                          (dt + timedelta(days=3) + MIN_TD * 3, 4),
                          ]


def test_recordable_feedback_replay():
    with GlobalState():
        set_recording_label("test")
        register_recorder_api(api := PolarsRecorderAPI())
        api.create_or_update_table_definition("test", compound_scalar(value=int))
        dt = datetime(2024, 1, 1)
        result = evaluate_graph(g, GraphConfiguration(start_time=dt, end_time=dt + timedelta(days=3)))
        assert result == [
            (dt + timedelta(days=0) + MIN_TD * 0, 1),
            (dt + timedelta(days=1) + MIN_TD * 1, 2),
            (dt + timedelta(days=2) + MIN_TD * 2, 3),
        ]

        result = evaluate_graph(g, GraphConfiguration(
            start_time=dt + timedelta(days=2),
            end_time=dt + timedelta(days=4),
            trace=True
        ))
        assert result == [
            (dt + timedelta(days=2) + MIN_TD * 0, 3),
            (dt + timedelta(days=3) + MIN_TD * 1, 4),
        ]
