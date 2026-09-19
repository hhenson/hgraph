"""Completed-day component recovery through the native persistence contract."""

from datetime import datetime, timedelta

import hgraph as hg
import hgraph_persistence as persistence
import pytest


class RunningState(hg.TimeSeriesSchema):
    total: hg.TS[int]


@hg.compute_node
def running_total(ts: hg.TS[int], state: hg.RECORDABLE_STATE[RunningState] = None) -> hg.TS[int]:
    if ts.value == -999:
        raise RuntimeError("strategy computation failed")
    value = (state.total.value if state.total.valid else 0) + ts.value
    state.total.value = value
    return value


@running_total.start
def running_total_start(state: hg.RECORDABLE_STATE[RunningState] = None):
    if not state.total.valid:
        state.total.value = 0


@running_total.stop
def running_total_stop(state: hg.RECORDABLE_STATE[RunningState] = None):
    if state.total.value < 0:
        raise RuntimeError("strategy stop failed")


@hg.component(recordable_id="strategy")
def strategy(ts: hg.TS[int]) -> hg.TS[int]:
    return running_total(ts, __recordable_id__="total")


@hg.component(recordable_id="mapped-strategy")
def mapped_strategy(ts: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
    return hg.map_(running_total, ts)


def run_day(store, key, values, *, restore=None, start=0, end=None, revision="1"):
    end = start + len(values) if end is None else end
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(
            store, "strategy", key, restore, revision=revision, global_state=state)
        return hg.eval_node(
            strategy, values, __start_time__=hg.MIN_ST + hg.MIN_TD * start,
            __end_time__=hg.MIN_ST + hg.MIN_TD * end)


def test_completed_day_restores_hidden_state_without_a_recovery_tick(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)
    assert run_day(store, "day-one", [1, 2]) == [1, 3]
    assert store.contains("day-one")

    # New store and GlobalState instances use only the explicitly selected
    # on-disk predecessor. The first resumed cycle has no fresh input.
    reopened = persistence.ComponentCheckpointStore(tmp_path)
    assert run_day(reopened, "day-two", [None, 3],
                   restore="day-one", start=2) == [None, 6]
    assert reopened.contains("day-two")


def test_failed_day_does_not_publish_and_previous_day_can_be_retried(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)
    assert run_day(store, "day-one", [1, 2]) == [1, 3]
    with pytest.raises(RuntimeError, match="strategy computation failed"):
        run_day(store, "day-two", [3, -999],
                restore="day-one", start=2)
    assert not store.contains("day-two")
    assert run_day(store, "day-two", [3, 4],
                   restore="day-one", start=2) == [6, 10]


def test_failed_stop_does_not_publish(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)
    with pytest.raises(RuntimeError, match="strategy stop failed"):
        run_day(store, "failed", [-1])
    assert not store.contains("failed")


def test_hidden_state_change_is_saved_even_without_a_public_output(tmp_path):
    @hg.compute_node
    def hidden_total(ts: hg.TS[int], state: hg.RECORDABLE_STATE[RunningState] = None) -> hg.TS[int]:
        state.total.value = (state.total.value if state.total.valid else 0) + ts.value
        if ts.value != 5:
            return state.total.value

    @hg.component(recordable_id="hidden-output")
    def component(ts: hg.TS[int]) -> hg.TS[int]:
        return hidden_total(ts)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "hidden-output", "one", global_state=state)
        assert hg.eval_node(component, [1, 5],
                            __end_time__=hg.MIN_ST + 2 * hg.MIN_TD) == [1, None]
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "hidden-output", "two", "one", global_state=state)
        assert hg.eval_node(component, [2],
                            __start_time__=hg.MIN_ST + 2 * hg.MIN_TD,
                            __end_time__=hg.MIN_ST + 3 * hg.MIN_TD) == [8]


def test_recovery_refuses_missing_revision_or_duplicate_checkpoint(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)
    assert run_day(store, "day-one", [1]) == [1]
    with pytest.raises(RuntimeError, match="already exists"):
        run_day(store, "day-one", [2])
    with pytest.raises(RuntimeError, match="not found"):
        run_day(store, "missing-result", [2], restore="missing", start=1)
    with pytest.raises(RuntimeError):
        run_day(store, "changed-result", [2], restore="day-one", start=1,
                revision="changed-strategy-code")
    assert not store.contains("missing-result")
    assert not store.contains("changed-result")


def test_mapped_membership_and_child_state_survive_silent_restart_and_removal(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)

    def mapped_day(key, values, start, previous=None):
        # Emit future deltas through the normal runtime source. eval_node's
        # list pre-conversion validates REMOVE against a fresh temporary TSD,
        # before component recovery can restore its ingress baseline.
        @hg.generator
        def future_inputs() -> hg.TSD[str, hg.TS[int]]:
            for offset, value in enumerate(values):
                if value is not None:
                    yield hg.MIN_ST + (start + offset) * hg.MIN_TD, value

        @hg.graph
        def application() -> hg.TSD[str, hg.TS[int]]:
            return mapped_strategy(future_inputs())

        with hg.GlobalState() as state:
            persistence.configure_component_recovery(
                store, "mapped-strategy", key, previous, global_state=state)
            return hg.eval_node(
                application,
                __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                __end_time__=hg.MIN_ST + (start + len(values)) * hg.MIN_TD)

    assert mapped_day("one", [{"a": 1, "b": 10}, {"a": 2}], 0) == [
        {"a": 1, "b": 10}, {"a": 3}]
    assert mapped_day("two", [None, {"b": 5}, {"a": hg.REMOVE}],
                      2, "one") == [None, {"b": 15}, {"a": hg.REMOVE}]
    assert mapped_day("three", [{"a": 4, "b": 1}], 5, "two") == [
        {"a": 4, "b": 16}]


@pytest.mark.parametrize("injectable", [hg.STATE, hg.CLOCK, hg.GlobalState, hg.NODE])
def test_checkpoint_eligibility_uses_actual_python_injectables(tmp_path, injectable):
    @hg.compute_node
    def unsupported(ts: hg.TS[int], resource: injectable = None) -> hg.TS[int]:
        return ts.value

    @hg.component(recordable_id="unsupported")
    def component(ts: hg.TS[int]) -> hg.TS[int]:
        return unsupported(ts)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "unsupported", "day", global_state=state)
        with pytest.raises((RuntimeError, ValueError), match="unsupported"):
            hg.eval_node(component, [1], __end_time__=hg.MIN_ST + hg.MIN_TD)
    assert not store.contains("day")


@pytest.mark.parametrize("phase", ["eval", "start", "stop"])
def test_checkpoint_refuses_undeclared_closure_state(tmp_path, phase):
    captured = [0]

    @hg.compute_node
    def stateless(ts: hg.TS[int]) -> hg.TS[int]:
        return ts.value

    @hg.compute_node
    def hidden(ts: hg.TS[int]) -> hg.TS[int]:
        captured[0] += ts.value
        return captured[0]

    def lifecycle():
        captured[0] += 1

    node = hidden if phase == "eval" else stateless
    if phase != "eval":
        getattr(node, phase)(lifecycle)

    @hg.component(recordable_id="closure")
    def component(ts: hg.TS[int]) -> hg.TS[int]:
        return node(ts)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "closure", "day", global_state=state)
        with pytest.raises((RuntimeError, ValueError), match="captured closure state"):
            hg.eval_node(component, [1], __end_time__=hg.MIN_ST + hg.MIN_TD)
    assert not store.contains("day")


def test_duplicate_explicit_node_identity_is_rejected(tmp_path):
    @hg.component(recordable_id="duplicate")
    def duplicate(ts: hg.TS[int]) -> hg.TS[int]:
        first = running_total(ts, __recordable_id__="same")
        second = running_total(ts + ts, __recordable_id__="same")
        return first + second

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "duplicate", "day", global_state=state)
        with pytest.raises((RuntimeError, ValueError), match="duplicate"):
            hg.eval_node(duplicate, [1], __end_time__=hg.MIN_ST + hg.MIN_TD)
    assert not store.contains("day")


@pytest.mark.parametrize("mapped", [False, True])
def test_error_capture_cannot_hide_failure_or_drop_managed_node_ownership(tmp_path, mapped):
    if mapped:
        @hg.component(recordable_id="caught-error")
        def component(ts: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
            result = hg.map_(running_total, ts)
            hg.exception_time_series(result)
            return result

        values = [{"a": 1}]
    else:
        @hg.component(recordable_id="caught-error")
        def component(ts: hg.TS[int]) -> hg.TS[int]:
            result = running_total(ts)
            hg.exception_time_series(result)
            return result

        values = [1]

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "caught-error", "day", global_state=state)
        with pytest.raises((RuntimeError, ValueError), match="error capture is unsupported"):
            hg.eval_node(component, values, __end_time__=hg.MIN_ST + hg.MIN_TD)
    assert not store.contains("day")


def test_stateless_python_scalar_configuration_is_checked_on_restore(tmp_path):
    @hg.compute_node
    def multiply(ts: hg.TS[int], factor: int) -> hg.TS[int]:
        return ts.value * factor

    @hg.component(recordable_id="scalar-config")
    def component(ts: hg.TS[int], factor: int) -> hg.TS[int]:
        return multiply(ts, factor)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "scalar-config", "one", global_state=state)
        assert hg.eval_node(component, [2], factor=3,
                            __end_time__=hg.MIN_ST + hg.MIN_TD) == [6]
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "scalar-config", "two", "one", global_state=state)
        with pytest.raises(RuntimeError, match="incompatible"):
            hg.eval_node(component, [2], factor=4,
                         __start_time__=hg.MIN_ST + hg.MIN_TD,
                         __end_time__=hg.MIN_ST + hg.MIN_TD * 2)
    assert not store.contains("two")


@pytest.mark.parametrize("kind", ["freeze", "until_true", "python_until_true"])
def test_passivated_inputs_remain_passive_after_durable_restart(tmp_path, kind):
    @hg.compute_node
    def python_until_true(value: hg.TS[bool]) -> hg.TS[bool]:
        if value.value:
            value.make_passive()
        return value.value

    if kind == "freeze":
        @hg.component(recordable_id="activity-strategy")
        def component(predicate: hg.TS[bool], value: hg.TS[int]) -> hg.TS[int]:
            return hg.freeze(predicate, value)

        first_inputs = ([False, True], [1, 2])
        next_inputs = ([False, False], [3, 4])
        first_output = [1, 2]
    else:
        @hg.component(recordable_id="activity-strategy")
        def component(value: hg.TS[bool]) -> hg.TS[bool]:
            return python_until_true(value) if kind == "python_until_true" else hg.until_true(value)

        first_inputs = ([False, True],)
        next_inputs = ([False, False],)
        first_output = [False, True]

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "activity-strategy", "one", global_state=state)
        assert hg.eval_node(component, *first_inputs,
                            __end_time__=hg.MIN_ST + 2 * hg.MIN_TD) == first_output
    reopened = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(reopened, "activity-strategy", "two", "one", global_state=state)
        assert hg.eval_node(component, *next_inputs,
                            __start_time__=hg.MIN_ST + 2 * hg.MIN_TD,
                            __end_time__=hg.MIN_ST + 4 * hg.MIN_TD) is None
    assert reopened.contains("two")


@hg.compute_node
def pending_alarms(ts: hg.TS[int], scheduler: hg.SCHEDULER = None) -> hg.TS[int]:
    if ts.modified and ts.value == -1:
        scheduler.un_schedule("second")
        scheduler.schedule(hg.MIN_TD, "first")
    if scheduler.is_scheduled_now and scheduler.has_tag("first"):
        return ts.value
    if scheduler.is_scheduled_now and scheduler.has_tag("second"):
        return ts.value * 2


@pending_alarms.start
def pending_alarms_start(scheduler: hg.SCHEDULER = None):
    scheduler.schedule(hg.MIN_TD * 2, "first")
    scheduler.schedule(hg.MIN_TD * 4, "second")
    scheduler.schedule(hg.MIN_TD * 6, "cancelled")
    scheduler.un_schedule("cancelled")


@hg.component(recordable_id="alarms")
def alarm_component(ts: hg.TS[int]) -> hg.TS[int]:
    return pending_alarms(ts)


@pytest.mark.parametrize("replace", [False, True])
def test_pending_scheduler_restores_without_recordable_state(tmp_path, replace):
    def day(key, previous, start, end, values):
        with hg.GlobalState() as state:
            persistence.configure_component_recovery(
                persistence.ComponentCheckpointStore(tmp_path), "alarms", key,
                previous, global_state=state)
            return hg.eval_node(alarm_component, values,
                                __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                                __end_time__=hg.MIN_ST + end * hg.MIN_TD)

    assert day("one", None, 0, 2, [7, None]) is None
    if replace:
        assert day("two", "one", 2, 7, [-1, None, None, None, None]) == [None, -1, None, None, None]
    else:
        assert day("two", "one", 2, 4, [None, None]) == [7, None]
        assert day("three", "two", 4, 5, [None]) == [14]
        assert day("four", "three", 5, 12, [None] * 7) is None


@hg.component(recordable_id="alarms")
def mapped_alarm_component(ts: hg.TSD[str, hg.TS[int]]) -> hg.TSD[str, hg.TS[int]]:
    return hg.map_(pending_alarms, ts)


def test_restored_child_scheduler_notifies_parent_graph(tmp_path):
    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "alarms", "one", global_state=state)
        assert hg.eval_node(mapped_alarm_component, [{"a": 7}, None],
                            __end_time__=hg.MIN_ST + hg.MIN_TD * 2) == [{}, None]
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "alarms", "two", "one", global_state=state)
        assert hg.eval_node(mapped_alarm_component, [None],
                            __start_time__=hg.MIN_ST + hg.MIN_TD * 2,
                            __end_time__=hg.MIN_ST + hg.MIN_TD * 5) == [{"a": 7}, None, {"a": 14}]


@pytest.mark.parametrize("mapped", [False, True])
def test_future_alarm_preserves_fresh_restart_tick(tmp_path, mapped):
    store = persistence.ComponentCheckpointStore(tmp_path)
    component = mapped_alarm_component if mapped else alarm_component
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "alarms", "one", global_state=state)
        result = hg.eval_node(component, [{"a": 7}] if mapped else [7],
                              __end_time__=hg.MIN_ST + hg.MIN_TD)
        assert result == ([{}] if mapped else None)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "alarms", "two", "one", global_state=state)
        result = hg.eval_node(component, ([{"a": -1}] if mapped else [-1]) + [None] * 4,
                              __start_time__=hg.MIN_ST + hg.MIN_TD,
                              __end_time__=hg.MIN_ST + hg.MIN_TD * 6)
        assert result == [None, {"a": -1} if mapped else -1, None, None, None]


class DerivedCache:
    total = None


def cached_strategy(typed):
    cache_type = hg.STATE[DerivedCache] if typed else hg.STATE

    @hg.compute_node
    def counter(ts: hg.TS[int], cache: cache_type = None,
                state: hg.RECORDABLE_STATE[RunningState] = None) -> hg.TS[int]:
        assert cache.total == state.total.value
        cache.total += ts.value
        state.total.value = cache.total
        if ts.value != 5:
            return cache.total

    @counter.start
    def start(cache: cache_type = None, state: hg.RECORDABLE_STATE[RunningState] = None):
        assert getattr(cache, "total", None) is None
        if not state.total.valid:
            state.total.value = 0
        cache.total = state.total.value

    @counter.stop
    def stop(cache: cache_type = None, state: hg.RECORDABLE_STATE[RunningState] = None):
        assert cache.total == state.total.value

    @hg.component(recordable_id="cached")
    def component(ts: hg.TS[int]) -> hg.TS[int]:
        return counter(ts)

    return component


@pytest.mark.parametrize("typed", [False, True])
def test_recordable_state_rebuilds_fresh_cache_after_each_restore(tmp_path, typed):
    strategy = cached_strategy(typed)
    for key, previous, start, values, expected in [
        ("one", None, 0, [1, 5], [1, None]),
        ("two", "one", 2, [None, 2], [None, 8]),
        ("three", "two", 4, [3], [11]),
    ]:
        with hg.GlobalState() as state:
            store = persistence.ComponentCheckpointStore(tmp_path)
            persistence.configure_component_recovery(store, "cached", key, previous, global_state=state)
            assert hg.eval_node(strategy, values,
                                __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                                __end_time__=hg.MIN_ST + (start + len(values)) * hg.MIN_TD) == expected
        assert store.contains(key)


@hg.compute_node
def cached_alarms(ts: hg.TS[int], cache: hg.STATE[DerivedCache] = None,
                  state: hg.RECORDABLE_STATE[RunningState] = None,
                  scheduler: hg.SCHEDULER = None) -> hg.TS[int]:
    assert cache.total == state.total.value
    if ts.modified:
        cache.total += ts.value
        state.total.value = cache.total
    if scheduler.is_scheduled_now and scheduler.has_tag("publish"):
        return cache.total


@cached_alarms.start
def cached_alarms_start(cache: hg.STATE[DerivedCache] = None,
                        state: hg.RECORDABLE_STATE[RunningState] = None,
                        scheduler: hg.SCHEDULER = None):
    assert cache.total is None
    if not state.total.valid:
        state.total.value = 0
    cache.total = state.total.value
    scheduler.schedule(hg.MIN_TD * 2, "publish")


@hg.component(recordable_id="cached-alarms")
def cached_alarm_component(ts: hg.TS[int]) -> hg.TS[int]:
    return cached_alarms(ts)


def test_pending_alarm_restores_beside_rebuilt_cache_and_recordable_state(tmp_path):
    for key, previous, start, values, expected in [
        ("one", None, 0, [3], None),
        ("two", "one", 1, [None, None], [None, 3]),
        ("three", "two", 3, [None, None, None], None),
    ]:
        with hg.GlobalState() as state:
            store = persistence.ComponentCheckpointStore(tmp_path)
            persistence.configure_component_recovery(store, "cached-alarms", key, previous, global_state=state)
            assert hg.eval_node(cached_alarm_component, values,
                                __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                                __end_time__=hg.MIN_ST + (start + len(values)) * hg.MIN_TD) == expected
        assert store.contains(key)


@pytest.mark.parametrize("initial_delay", [False, True])
@pytest.mark.parametrize("max_ticks", [0, 3])
def test_schedule_restores_remaining_emissions_at_original_deadlines(tmp_path, initial_delay, max_ticks):
    @hg.component(recordable_id="periodic")
    def periodic() -> hg.TS[bool]:
        return hg.schedule(hg.MIN_TD * 2, initial_delay=initial_delay, max_ticks=max_ticks)

    first = 2 if initial_delay else 0
    intervals = [(0, first + 1), (first + 1, first + 3), (first + 3, first + 5),
                 (first + 5, first + 8), (20, 23)]
    previous = None
    for index, (start, end) in enumerate(intervals):
        with hg.GlobalState() as state:
            store = persistence.ComponentCheckpointStore(tmp_path)
            key = str(index)
            persistence.configure_component_recovery(store, "periodic", key, previous, global_state=state)
            actual = hg.eval_node(periodic, __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                                  __end_time__=hg.MIN_ST + end * hg.MIN_TD)
            expected = [True if tick in range(first, first + max_ticks * 2, 2) else None
                        for tick in range(start, end)]
            assert actual == (expected if any(expected) else None)
        assert store.contains(key)
        previous = key


@pytest.mark.parametrize("with_start", [False, True])
@pytest.mark.parametrize("max_ticks", [0, 3])
def test_schedule_restores_time_series_delay_progress_and_start_reset(tmp_path, with_start, max_ticks):
    @hg.component(recordable_id="periodic")
    def periodic(delay: hg.TS[timedelta], start: hg.TS[datetime]) -> hg.TS[bool]:
        if with_start:
            return hg.schedule(delay, start=start, max_ticks=max_ticks)
        return hg.schedule(delay, max_ticks=max_ticks)

    previous = None
    days = [(0, 3, [hg.MIN_TD * 2], [hg.MIN_ST], [None, None, True]),
            (3, 5, [None], [None], [None, True]), (5, 7, [None], [None], [None, True]),
            (20, 23, [None], [None], None)]
    if max_ticks == 0:
        days = [(0, 1, [hg.MIN_TD * 2], [hg.MIN_ST], None), (20, 23, [None], [None], None)]
    if with_start:
        days.append((23, 30, [None], [hg.MIN_ST + hg.MIN_TD * 23], [None, None, True, None, True, None, True]))
    for index, (start, end, delays, starts, expected) in enumerate(days):
        if max_ticks == 0:
            expected = None
        with hg.GlobalState() as state:
            store = persistence.ComponentCheckpointStore(tmp_path)
            key = str(index)
            persistence.configure_component_recovery(store, "periodic", key, previous, global_state=state)
            assert hg.eval_node(periodic, delays, starts,
                                __start_time__=hg.MIN_ST + start * hg.MIN_TD,
                                __end_time__=hg.MIN_ST + end * hg.MIN_TD) == expected
        previous = key


def test_schedule_checkpoint_restores_first_deadline_and_checks_configuration(tmp_path):
    @hg.component(recordable_id="periodic")
    def periodic(delay: timedelta = hg.MIN_TD * 2, max_ticks: int = 3) -> hg.TS[bool]:
        return hg.schedule(delay, max_ticks=max_ticks)

    store = persistence.ComponentCheckpointStore(tmp_path)
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "periodic", "one", global_state=state)
        assert hg.eval_node(periodic, __end_time__=hg.MIN_ST + hg.MIN_TD * 2) is None
    for kwargs in ({"delay": hg.MIN_TD * 3}, {"max_ticks": 4}):
        with hg.GlobalState() as state:
            persistence.configure_component_recovery(store, "periodic", "changed", "one", global_state=state)
            with pytest.raises(RuntimeError, match="incompatible"):
                hg.eval_node(periodic, **kwargs, __start_time__=hg.MIN_ST + hg.MIN_TD * 2,
                             __end_time__=hg.MIN_ST + hg.MIN_TD * 7)
        assert not store.contains("changed")
    with hg.GlobalState() as state:
        persistence.configure_component_recovery(store, "periodic", "two", "one", global_state=state)
        assert hg.eval_node(periodic, __start_time__=hg.MIN_ST + hg.MIN_TD * 2,
                            __end_time__=hg.MIN_ST + hg.MIN_TD * 7) == [True, None, True, None, True]
