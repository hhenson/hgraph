from datetime import datetime, timedelta, timezone

from frozendict import frozendict

from hgraph import (
    EvaluationEngineApi, EvaluationMode, TS, const, graph, run_graph, sink_node,
)
from hgraph.adaptors.data_catalogue import (
    FixedDelayRetryOptions, PollingScope, RetryScope, StringScope,
)
from hgraph.adaptors.delta.delta_subscriber import DeltaDataSource, _render_filters
from hgraph.adaptors.sql.sql_subscriber import SqlDataSource, _render_query
from hgraph.stream import StreamStatus


_DELAY = timedelta(milliseconds=10)


def _end_time():
    # Safety net only: every test stops its own engine once it has captured
    # what it asserts on, so the width costs nothing on the happy path. A
    # shared CI runner stalling longer than the remainder of the window
    # after a capture drops the next wall-clock poll and fails the test
    # (observed on windows-latest with the previous 2s window).
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=15)


def test_delta_subscription_polling_reissues_rendered_request():
    captured = []

    @sink_node
    def capture(value: TS[tuple], engine: EvaluationEngineApi = None):
        captured.append(value.value)
        if len(captured) == 3:
            engine.request_engine_stop()

    @graph
    def app():
        source = DeltaDataSource(
            source_path="delta", table="prices",
            query=(("symbol", "=", "symbol"),))
        scope = frozendict({
            "symbol": StringScope(),
            "poll": PollingScope(_DELAY),
        })
        capture(_render_filters(source, scope, {"symbol": "ABC"}))

    run_graph(app, run_mode=EvaluationMode.REAL_TIME, end_time=_end_time())
    assert captured == [(("symbol", "=", "ABC"),)] * 3


def test_sql_subscription_polling_reissues_rendered_request():
    captured = []

    @sink_node
    def capture(value: TS[str], engine: EvaluationEngineApi = None):
        captured.append(value.value)
        if len(captured) == 3:
            engine.request_engine_stop()

    @graph
    def app():
        source = SqlDataSource(source_path="sql", query="select '{symbol}'")
        scope = frozendict({
            "symbol": StringScope(),
            "poll": PollingScope(_DELAY),
        })
        capture(_render_query(source, scope, {"symbol": "ABC"}))

    run_graph(app, run_mode=EvaluationMode.REAL_TIME, end_time=_end_time())
    assert captured == ["select 'ABC'"] * 3


def test_sql_subscription_retries_after_error_feedback():
    captured = []

    @sink_node
    def capture(value: TS[str], engine: EvaluationEngineApi = None):
        captured.append(value.value)
        if len(captured) == 2:
            engine.request_engine_stop()

    @graph
    def app():
        source = SqlDataSource(source_path="sql", query="select '{symbol}'")
        retry = FixedDelayRetryOptions(delay=_DELAY, max_retries=1)
        scope = frozendict({
            "symbol": StringScope(),
            "retry": RetryScope(retry),
        })
        status = const(StreamStatus.ERROR, tp=TS[StreamStatus], delay=_DELAY)
        capture(_render_query(source, scope, {"symbol": "ABC"}, status))

    run_graph(app, run_mode=EvaluationMode.REAL_TIME, end_time=_end_time())
    assert captured == ["select 'ABC'", "select 'ABC'"]


def test_sql_subscription_does_not_reissue_after_success_feedback():
    captured = []

    @sink_node
    def capture(value: TS[str]):
        captured.append(value.value)

    @graph
    def app():
        source = SqlDataSource(source_path="sql", query="select '{symbol}'")
        scope = frozendict({"symbol": StringScope()})
        status = const(StreamStatus.OK, tp=TS[StreamStatus], delay=_DELAY)
        capture(_render_query(source, scope, {"symbol": "ABC"}, status))

    run_graph(
        app,
        run_mode=EvaluationMode.REAL_TIME,
        end_time=datetime.now(timezone.utc).replace(tzinfo=None) + 3 * _DELAY,
    )
    assert captured == ["select 'ABC'"]
